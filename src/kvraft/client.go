package kvraft

import (
	"crypto/rand"
	"math/big"
	"sync"

	"6.824/labrpc"
)


type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
    id int64
    requests int 
    mu sync.Mutex
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
    ck.id = nrand()
	return ck
}

//
// It's Ok to assume that one client (Clerk) only make one 
// call to a server at a time.
//
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
    ck.mu.Lock()
    defer ck.mu.Unlock()
    ck.requests += 1
    args := GetArgs {
        Client: ck.id,
        Seq: ck.requests,
        Key: key,
    }

    for {
        for server, _ := range ck.servers {
            // fmt.Printf("time %v: Request Get(%v) client %v\n", time.Now().UnixMilli(), key, ck.id)
            reply := GetReply{}
            ok := ck.servers[server].Call("KVServer.Get", &args, &reply)
            if !ok || reply.Err == ErrWrongLeader {
                continue
            }
            if server != 0 {
                ck.servers = append(ck.servers[server:], ck.servers[:server]...)
            }
            return reply.Value
        }
    }
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
    ck.mu.Lock()
    defer ck.mu.Unlock()
    ck.requests += 1
    args := PutAppendArgs {
        Client: ck.id,
        Seq: ck.requests,
        Key: key,
        Value: value,
        Op: op,
    }
    for {
        for server, _ := range ck.servers {
            // fmt.Printf("time %v: Request %v(%v, %v) client: %v\n", time.Now().UnixMilli(), op, key, value, ck.id)
            reply := PutAppendReply{}
            ok := ck.servers[server].Call("KVServer.PutAppend", &args, &reply)
            if !ok || reply.Err == ErrWrongLeader {
                continue
            }
            if server != 0 {
                ck.servers = append(ck.servers[server:], ck.servers[:server]...)
            }
            return
        }
    }
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
