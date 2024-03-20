package kvraft

import (
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

//
// Note:
// Don't try to send channel or conditional variable via RPC.
// It won't work because they are in memory data structure and 
// thus only work on a single process.
//
type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
    Args interface{}
}

type PendingEntry struct {
    seq int
    resultCh chan string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
    pending map[int64]PendingEntry


    /* lock-free structures only accessed by a single executor thread */
    Database map[string]string
    // The last Request Sequence Number seen / serviced for each client
    LastExecuted map[int64]int
}

/* Handle RPC request from client */ 

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
    op := Op {
        Args: *args,
    }

    kv.mu.Lock()
    _, _, isLeader := kv.rf.Start(op)

    if !isLeader {
        reply.Err = ErrWrongLeader
        kv.mu.Unlock()
        return
    }
    // fmt.Printf("time %v: server %v store Get(%v) at %v\n", time.Now().UnixMilli(), kv.me, args.Key, idx)

    //
    // We use buffered channel to avoid the case where we insert a 
    // channel, release the lock, but before we listen to the channel, 
    // the executor reply and our message lost. 
    //
    resultCh := make(chan string, 1)
    kv.pending[args.Client] = PendingEntry{
        seq: args.Seq,
        resultCh: resultCh,
    }
    // fmt.Printf("time %v: server %v insert channel at %v\n", time.Now().UnixMilli(), kv.me, idx)
    kv.mu.Unlock()

    // fmt.Printf("time %v: server %v start waiting at %v\n", time.Now().UnixMilli(), kv.me, idx)
    select {
    case result := <- resultCh:
        reply.Err = OK
        reply.Value = result

    // 
    // Set timeout in case of:
    // - The leader blocks commit old entry forever when no new entry comes 
    //   in (no-op mechanism can avoid this, but the tester don't allower:)
    // - Failure happens and the leader fails to commit our command.
    //
    case <- time.After(time.Millisecond * 200):
        reply.Err = ErrWrongLeader
        // fmt.Printf("server %v: timeout: stop waiting for entry at %v\n", kv.me, idx)
    }

    //
    // It is fine not to clear the channel since it will be replaced 
    // when the client sends another request to this server, 
    // and executor will ignore any channels that do not match the 
    // current executing command. 
    // However, clearing the channel can reduce the times the executor 
    // checks for the channel pending.id notably.
    //
    kv.mu.Lock()
    delete(kv.pending, args.Client)
    // fmt.Printf("time %v: server %v clearing channel at %v\n", time.Now().UnixMilli(), kv.me, idx)
    kv.mu.Unlock()
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
    op := Op {
        Args: *args,
    }

    kv.mu.Lock()
   // fmt.Printf("time %v: server %v received %v(%v, %v)\n", time.Now().UnixMilli(), kv.me, args.Op, args.Key, args.Value)

    _, _, isLeader := kv.rf.Start(op)

    if !isLeader {
        // fmt.Printf("time %v: server %v not the leader\n", time.Now().UnixMilli(), kv.me)
        reply.Err = ErrWrongLeader
        kv.mu.Unlock()
        return
    }
    // fmt.Printf("time %v: server %v store %v(%v, %v) at %v\n", time.Now().UnixMilli(), kv.me, args.Op, args.Key, args.Value, idx)
    resultCh := make(chan string, 1)
    kv.pending[args.Client] = PendingEntry{
        seq: args.Seq,
        resultCh: resultCh,
    }
    // fmt.Printf("time %v: server %v insert channel at %v\n", time.Now().UnixMilli(), kv.me, idx)
    kv.mu.Unlock()

    // fmt.Printf("time %v: server %v start waiting at %v\n", time.Now().UnixMilli(), kv.me, idx)
    select {
    case <- resultCh:
        reply.Err = OK
        // fmt.Printf("time %v: server %v replying op at index %v\n", time.Now().UnixMilli(), kv.me, idx)

    case <- time.After(time.Millisecond * 200):
        // fmt.Printf("server %v: timeout: stop waiting for entry at %v\n", kv.me, idx)
        reply.Err = ErrWrongLeader
    }

    kv.mu.Lock()
    delete(kv.pending, args.Client)
    // fmt.Printf("time %v: server %v clearing channel at %v\n", time.Now().UnixMilli(), kv.me, idx)
    kv.mu.Unlock()
}

//
// Execute the commands return from the Raft applyCh linearly.
// Only the leader needs to reply. 
// Executor can only reply success. It ignores any channel that 
// does not match the current command.
// 
// Write requests won't be re-executed. If executor receives a duplicated
// write request, it simply ignores the request and does not perform any
// database operation. However, if the duplicated request is a read
// one, it should be re-executed if the client is waiting for the reply.
//
// A duplicated request should reply success to the client if the
// client is waiting for it **exactly** (not the other one), because 
// this indicates that the client does not know its request has 
// been serviced.
//
func (kv *KVServer) executor(applyCh chan raft.ApplyMsg) {
    for msg := range applyCh {
        if kv.killed() {
            return
        }
        if msg.CommandValid {
            // Before the command is executed, check whether
            // a snapshot is needed.
            if kv.maxraftstate != -1 && kv.rf.RaftStatedSize() > kv.maxraftstate / 4 * 3 {
                snapshot := kv.createSnapshot()
                kv.rf.Snapshot(msg.CommandIndex - 1, snapshot)
            }

            //
            // Can happen:
            // - The server receives op whose args.Seq <= LastExecuted,
            //   but no one is waiting for it to reply (the 
            //   server is not the leader)
            // - The server receives op whose args.Seq == LastExecuted,
            //   and the client is waiting for this server to reply
            // 
            // Can never happen:
            // - The server receives op whose args.Seq < LastExecuted,
            //   but someone is waiting for it to reply,
            // (Client only makes one call to a server at 
            // a time, and thus won't make request with new id 
            // until the previous one return)
            // 
            // If args.Seq == LastExecuted and the client is waiting,
            // the server should to let the client know that its 
            // request is serviced and thus is free to make new 
            // requests again.
            //
            // In such case:
            // - For a read request, the server should re-read from 
            //   database and reply the value it request.
            //   Note that the "re-read" request can yield value
            //   different from the previous one since it is executed
            //   at a different timestamp. But that's fine because 
            //   the client doesn't know about the previous value,
            //   or it won't be waiting for this request to be serviced.
            // - For a write request, the server should simply 
            //   and do not perform any database operations.
            //
            op := msg.Command.(Op)
            switch args:= op.Args.(type) {
            case GetArgs:
                lastExecuted := kv.LastExecuted[args.Client]
                if args.Seq > lastExecuted {
                    // fmt.Printf("time %v: server %v executed Get(%v) at %v\n", time.Now().UnixMilli(), kv.me, args.Key, msg.CommandIndex)
                    kv.LastExecuted[args.Client] = args.Seq
                }

                kv.mu.Lock()
                pending, ok := kv.pending[args.Client]
                kv.mu.Unlock()
                //
                // It is possible that ok && pending.seq != args.Seq.
                // In such a case, we should simply ignore the channel,
                // but should not clear the channel because the request
                // listened through the channel is very likely to be 
                // serviced later.
                //
                // Suppose there are one client C and two servers A and B.
                // Suppose C submits a command U to A, but A lost leadership 
                // before it makes it to commits. C retries and submits
                // the command U to the new leader B. 
                // B commits command U successfully, and C returns.
                // C submits another command V to B, but this time B fails,
                // and A becomes the leader. C retries sending command V 
                // to A, waiting for the reply. But at this time 
                // A just start to execute U. 
                //
                if ok && pending.seq == args.Seq {
                    if args.Seq < lastExecuted {
                        panic("one client made another type of request before return")
                    }
                    value := kv.Database[args.Key]
                    select {
                    case pending.resultCh <- value:
                        // fmt.Printf("time %v: server %v notify executed at %v\n", time.Now().UnixMilli(), kv.me, msg.CommandIndex)
                    default:

                    }
                }

            case PutAppendArgs: 
                lastExecuted := kv.LastExecuted[args.Client]

                if args.Seq > lastExecuted {
                    // fmt.Printf("time %v: server %v executed %v(%v, %v) at %v\n", time.Now().UnixMilli(), kv.me, args.Op, args.Key, args.Value, msg.CommandIndex)
                    key := args.Key
                    dbvalue := kv.Database[key]
                    if args.Op == "Put" {
                        dbvalue = args.Value
                    } else {
                        dbvalue += args.Value
                    }
                    kv.Database[key] = dbvalue
                    kv.LastExecuted[args.Client] = args.Seq
                }

                kv.mu.Lock()
                pending, ok := kv.pending[args.Client]
                kv.mu.Unlock()

                if ok && pending.seq == args.Seq {
                    if args.Seq < lastExecuted {
                        panic("one client made another type of request before the previous one return")
                    }
                    select {
                    case pending.resultCh <- "":
                        // fmt.Printf("time %v: server %v notify executed at %v\n", time.Now().UnixMilli(), kv.me, msg.CommandIndex)
                    default:
                    }
                }
            }
        } else {
            kv.installSnapshot(msg.SnapshotIndex, msg.Snapshot)
        }
    }
}


//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *KVServer) installSnapshot(index int, data []byte) {
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
    var database map[string]string
    var lastExecuted map[int64]int
	if d.Decode(&database) != nil ||
	   d.Decode(&lastExecuted) != nil {
        panic("server persistent state is broken")
    }
    kv.Database = database
    kv.LastExecuted = lastExecuted
}

func (kv *KVServer) createSnapshot() []byte {
    w := new(bytes.Buffer)
    e := labgob.NewEncoder(w)
    e.Encode(kv.Database)
    e.Encode(kv.LastExecuted)
    data := w.Bytes()
    return data
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})
    labgob.Register(GetArgs{})
    labgob.Register(PutAppendArgs{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
    kv.pending = make(map[int64]PendingEntry)
    kv.LastExecuted = make(map[int64]int)
    kv.Database = make(map[string]string)

    go kv.executor(kv.applyCh)

	return kv
}
