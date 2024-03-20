package raft

//
// This is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
)

//
// As each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type Entry struct {
	Command interface{}
    Term int
}

type ServerState int

const (
    Candidate ServerState = iota
    Follower ServerState = iota
    Leader ServerState = iota
)

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

    /* COMMON VOLATILE STATE */

    // Send message to this channel to apply the log entry, or 
    // the snapshot installed from the leader, to state machine.
    applyCh chan ApplyMsg
    // Notify the applier to apply entries if not currently 
    // snapshoting and rf.commitIndex > rf.lastApplied
    applierCond *sync.Cond
    // Index of the highest log entry.
    lastLogIndex int
    // Index of highest log entry known to be committed. Initialized to 0,
    // or snapshot LastIncludedIndex when reboot.
    commitIndex int
    // Index of highest log entry known to be applied. Initialized to 0,
    // or snapshot LastIncludedIndex when reboot.
    lastApplied int
    // Server state: Leader, Follower or Candidate
    state ServerState
    // An atomic state: 1 if election timeout elapses, and 0 otherwise
    timeoutElapses int32
    // snapshot pending to be applied
    pendingSnapshot []byte

    /* LEADER ONLY VOLATILE STATE */

    // For each server, index of the next log entry to send to that 
    // server (initialized to lastLogIndex + 1 when elected as leader)
    nextIndex []int
    // For each server, index of the highest log entry **known** to 
    // be replicated on that server (initialized to 0 when elected as leader)
    matchIndex []int

    /* COMMON PERSISTENT STATE */

    // Latest term this server has seen (initialized to 0)
    CurrentTerm int
    // Id of candidate that grants its vote in currentTerm (or -1 if none)
    VotedFor int
    // Log entries (index starting from 1, Log[0] is trivial, with 
    // Log[0].Term initialized to 0)
    Log map[int]Entry
    // The index of the last entry in the log that the snapshot replaces
    LastIncludedIndex int
}

//
// Generate a slice of log starting at startIndex, sorted by index.
//
func log2slice(log map[int]Entry, startIndex int) []Entry {
    var values []Entry
    var keys []int
    for k, _ := range log {
        if k >= startIndex {
            keys = append(keys, k)
        }
    }
    sort.Ints(keys)
    for _, k := range keys {
        values = append(values, log[k])
    }
    return values
}

//
// Return CurrentTerm and whether this server
// believes it is the leader.
//
func (rf *Raft) GetState() (int, bool) {
    rf.mu.Lock()
    defer rf.mu.Unlock()
	return rf.CurrentTerm, rf.state == Leader
}

//
// Save persistent state before it can do anything else to observe 
// the new state, i.e., before sending an RPC, replying to an RPC, 
// returning from Start(), or applying a command to the state machine.
//
func (rf *Raft) persist(snapshot []byte) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.CurrentTerm)
	e.Encode(rf.VotedFor)
	e.Encode(rf.Log)
	e.Encode(rf.LastIncludedIndex)
	data := w.Bytes()
    if len(snapshot) != 0 {
	    rf.persister.SaveStateAndSnapshot(data, snapshot)
    } else {
        rf.persister.SaveRaftState(data)
    }
}

//
// Restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
    var CurrentTerm int
    var VotedFor int
    var Log map[int]Entry
    var LastIncludedIndex int
	if d.Decode(&CurrentTerm) != nil ||
	   d.Decode(&VotedFor) != nil || 
       d.Decode(&Log) != nil || 
       d.Decode(&LastIncludedIndex) != nil {
        panic("server persistent state is broken")
    }
    rf.CurrentTerm = CurrentTerm
    rf.VotedFor = VotedFor
    rf.Log = Log
    rf.LastIncludedIndex = LastIncludedIndex
}

//
// RequestVote RPC arguments structure.
//
type RequestVoteArgs struct {
    CandidateTerm int
    CandidateID int
    LastLogIndex int 
    LastLogTerm int
}

//
// RequestVote RPC reply structure.
//
type RequestVoteReply struct {
    VoteGranted bool
    UpdatedTerm int
}

//
// Update CurrentTerm, convert to a follower, 
// and set voteFor to none since CurrentTerm has changed.
// Called whenever discovering a server with higher term.
// Caller should makes sure that it grabs the lock.
// (Figure 2, rules for all servers)
//
func (rf *Raft) handleHigherTerm(newTerm int, persist bool) {
    rf.CurrentTerm = newTerm
    rf.VotedFor = -1
    rf.state = Follower
    if persist {
        rf.persist(nil)
    }
}

//
// RequestVote RPC handler.
//
func (rf *Raft) RequestVoteHandler(args *RequestVoteArgs, reply *RequestVoteReply) {
    rf.mu.Lock()
    defer rf.mu.Unlock()

    // Reply false if CandidateTerm < CurrentTerm 
    // (Figure 2, RequestVote RPC)
    if args.CandidateTerm < rf.CurrentTerm {
        reply.VoteGranted = false
        reply.UpdatedTerm = rf.CurrentTerm
        // fmt.Printf("server %v rejects the RequestVote RPC from server %v as currentTerm %v > candidateTerm %v\n", rf.me, args.CandidateID, rf.CurrentTerm, args.CandidateTerm)
        return
    } 

    // Updates CurrentTerm if RPC request contains a 
    // higher term (Figure 2, rules for all servers)
    if args.CandidateTerm > rf.CurrentTerm {
        rf.handleHigherTerm(args.CandidateTerm, true)
    }

    // If votedFor is neither none nor CandidateID, 
    // reply false (Figure 2, RequestVote RPC)
    if rf.VotedFor != -1 && rf.VotedFor != args.CandidateID {
        reply.VoteGranted = false
        // fmt.Printf("server %v rejects the RequestVote RPC from server %v as votedFor = %v\n", rf.me, args.CandidateID, rf.VotedFor)
        return
    }

    // In order to grant vote, the candidate must have log
    // at least as up-to-date as that of the receiver,
    // where "up-to-date" is defined as:
    // - If the logs have last entries with different terms,
    //   then the log with the later term is more up-to-date.
    // - If the logs end with the same term, then 
    //   whichever log is longer is more up-to-date.
    // (§5.4.1 && Figure 2, RequestVote RPC)
    if rf.Log[rf.lastLogIndex].Term > args.LastLogTerm {
        reply.VoteGranted = false
        // fmt.Printf("server %v rejects the RequestVote RPC from server %v as its log is more up-to-date\n", rf.me, args.CandidateID)
        return
    } else if rf.Log[rf.lastLogIndex].Term == args.LastLogTerm &&
       rf.lastLogIndex > args.LastLogIndex {
        reply.VoteGranted = false
        // fmt.Printf("server %v rejects the RequestVote RPC from server %v as its log is more up-to-date\n", rf.me, args.CandidateID)
        return
    }

    // Unset election timeout when granting vote for a candidate
    // (Figure 2, rules for followers)
    rf.unsetTimeout()

    // Grant the vote and reply success (Figure 2, RequestVote RPC)
    rf.VotedFor = args.CandidateID
    rf.persist(nil)
    reply.VoteGranted = true
}

// 
// Send RequestVote RPC to a peer.
// 
// RPCs can block indefinitely due to network latency, 
// power failure of the receiver and so on. So make sure that 
// the caller does not grab the lock, and remember to check 
// whether the caller's state has changed before the RPC 
// is sent and after the RPC replies if necessary.
// Also make sure that the caller takes the response 
// carefully since RPCs can reorder, lost or duplicate.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVoteHandler", args, reply)
	return ok
}

//
// Start an election, sending RequestVote RPC to all peers.
// 
// Each election has a session, which can be simply identified by the sender's 
// state. If a sender is no longer a canidate in this term, i.e., not a canidate,
// or a candidate but with different term, then the session is stale. 
// This can happen since it does not grab the lock while waiting for reply. 
// To ensure the consistency of the election session, the sender make sure that 
// the arguments of RequestVote RPC only contains the state at the point when 
// it attemps the election, and abort the election immediately without response
// to any of the replies if it realizes that the session is stale.
//
// Since RPC can duplicate, Raft RPCs should be idempotent. To ensure the idempotence 
// of RequestVote RPC, the sender uses map to stores the votes result in case of taking 
// duplicate votes from peer, which can happen since sender can vote for the one that it 
// has already voted for in the same term.
// 
func (rf *Raft) AttemptElection() {
    rf.mu.Lock()
    // fmt.Printf("server %v attempts election at term %v\n", rf.me, rf.CurrentTerm)
    // On conversion to candidate, start election:
    // 1. Increment CurrentTerm
    // 2. Vote for self
    // 3. Reset election timer
    // (§5.2 && Figure 2, rules for candidates)
    rf.state = Candidate
    rf.CurrentTerm += 1
    rf.VotedFor = rf.me
    rf.persist(nil)
    rf.unsetTimeout()

    currentTerm := rf.CurrentTerm
    lastLogindex := rf.lastLogIndex
    lastLogTerm := rf.Log[lastLogindex].Term
    rf.mu.Unlock()

    done := 1
    voteGrantedFrom := make(map[int]bool)
    voteGrantedFrom[rf.me] = true
    cond := sync.NewCond(&rf.mu)
    for sever := range rf.peers {
        if sever == rf.me {
            continue
        }
        var request func(int)
        request = func(server int) {
            args := RequestVoteArgs {
                CandidateID: rf.me,
                CandidateTerm: currentTerm, 
                LastLogIndex: lastLogindex,
                LastLogTerm: lastLogTerm,
            }
            var reply RequestVoteReply
            ok := rf.sendRequestVote(server, &args, &reply)

            rf.mu.Lock()
            defer rf.mu.Unlock()

            // Ignore the reply if it is no longer a candidate in this term
            if rf.killed() || rf.state != Candidate || 
               currentTerm != rf.CurrentTerm {
                done += 1
                if done == len(rf.peers) {
                    cond.Broadcast()
                }
                return
            }

            // Retry if RPC failed (§5.1 && §5.5)
            if !ok {
                go request(server)
                return
            }

            if reply.VoteGranted {
                voteGrantedFrom[server] = true
                done += 1
                if done == len(rf.peers) || 
                   len(voteGrantedFrom) > len(rf.peers) / 2 {
                    cond.Broadcast()
                }
            } else {
                // Updates CurrentTerm if RPC response contains a
                // higher term (Figure 2, rules for all servers)
                if reply.UpdatedTerm > rf.CurrentTerm {
                    rf.handleHigherTerm(reply.UpdatedTerm, true)
                }
                done += 1
                if done == len(rf.peers) {
                    cond.Broadcast()
                }
            }

        }
        go request(sever)
    }

    rf.mu.Lock()
    defer rf.mu.Unlock()  
    for !rf.killed() && rf.state == Candidate &&
        currentTerm == rf.CurrentTerm &&
        done < len(rf.peers) && len(voteGrantedFrom) <= len(rf.peers) / 2 {
        cond.Wait()
    }

    // If votes received from majority of servers:
    // become leader (§5.2 && Figure 2, rules for candidates)
    if !rf.killed() && rf.state == Candidate &&
       rf.CurrentTerm == currentTerm && len(voteGrantedFrom) > len(rf.peers) / 2 {
        rf.state = Leader
        // fmt.Printf("server %v is elected as leader\n", rf.me)
        rf.nextIndex = make([]int, len(rf.peers))
        rf.matchIndex = make([]int, len(rf.peers))
        for server, _ := range rf.peers {
            rf.nextIndex[server] = rf.lastLogIndex + 1
        }
        go rf.maintainAuthority()
    }
}

// 
// Mark the election timeout as elapses
//
func (rf *Raft) setTimeout() {
    atomic.StoreInt32(&rf.timeoutElapses, 1)
}

// 
// Unmark the election timeout
//
func (rf *Raft) unsetTimeout() {
    atomic.StoreInt32(&rf.timeoutElapses, 0)
}

//
// Whether election timeout elapses
//
func (rf *Raft) electionTimeout() bool {
    return atomic.LoadInt32(&rf.timeoutElapses) == 1
}

type AppendEntriesArgs struct {
    LeaderTerm int
    LeaderId int
    // The lastLogIndex of receiver expected by the leader
    PrevLogIndex int
    // The term of entry at lastLogIndex in leader's log.
    PrevLogTerm int
    Entries []Entry
    LeaderCommitIndex int
}

type AppendEntriesReply struct {
    UpdatedTerm int
    Success bool
    // The index of the first entry with same term as the conflicted entry.
    // Whenever AppendEntries RPC fails due to log inconsistency, the server 
    // set rf.nextIndex[server] to this value. 
    // With this optimization, one RPC will be required for each term 
    // with conflicting entries, rather than one RPC per entry. (§5.3)
    RetryIndex int
}

// 
// AppendEntries RPC handler.
// When arg.entries == nil, serve as heartsbeats, 
// but heartsbeats is only handled implicitly (and they are also sent implicitly).
// 
// Raft RPCs are idempotent (§5.5). Following the rules in Figure 2 
// automatically ensures the idempotence of AppendEntries RPC in the receiver's side.
// 
func (rf *Raft) AppendEntriesHandler(args *AppendEntriesArgs, reply *AppendEntriesReply) {
    rf.mu.Lock()
    defer rf.mu.Unlock()
    if len(args.Entries) != 0 {
        // fmt.Printf("server %v received %v entries at index %v\n", rf.me, len(args.Entries), args.PrevLogIndex + 1)
    }

    // Reply false if args.Term < CurrentTerm (Figure 2, AppendEntries RPC)
    if rf.CurrentTerm > args.LeaderTerm {
        reply.UpdatedTerm = rf.CurrentTerm
        reply.Success = false
        // fmt.Printf("server %v rejects AppendEntries RPC from server %v as currentTerm > leader's Term\n", rf.me, args.LeaderId)
        return
    } 

    // Reply false if log does not contain an entry at args.PrevLogIndex
    // whose term matches args.PrevLogTerm (Figure 2, AppendEntries RPC),
    if _, ok := rf.Log[args.PrevLogIndex]; !ok {
        reply.Success = false
        reply.RetryIndex = rf.lastLogIndex + 1
        // fmt.Printf("server %v rejects %v entries at %v from server %v due to log inconsistency\n", rf.me, len(args.Entries), args.PrevLogIndex + 1, args.LeaderId)
        return
    } else if rf.Log[args.PrevLogIndex].Term != args.PrevLogTerm {
        reply.Success = false
        retryIndex := args.PrevLogIndex
        conflictedTerm := rf.Log[args.PrevLogIndex].Term
        for _, ok := rf.Log[retryIndex]; ok && 
           rf.Log[retryIndex].Term == conflictedTerm; {
            retryIndex -= 1
        }
        retryIndex += 1
        reply.RetryIndex = retryIndex
        // fmt.Printf("server %v rejects %v entries at %v from server %v due to log inconsistency\n", rf.me, len(args.Entries), args.PrevLogIndex + 1, args.LeaderId)
        return
    }

    persist := false

    // Update CurrentTerm if RPC request contains a higher term
    // (Figure 2, rules for all servers)
    if args.LeaderTerm > rf.CurrentTerm {
        rf.handleHigherTerm(args.LeaderTerm, false)
        persist = true
    }

    // Maintain the authority of the **current** leader (at least this server believes), 
    // which is ensured by the above sanity checks (Figure 2, rules for followers && §5.2)
    rf.unsetTimeout()
    rf.state = Follower

    // If an existing entry **conflicts** with a new one (same index but 
    // different terms), delete the existing entry and all that follow it. 
    // (Figure 2, AppendEntries RPC)
    // Abiding this rule, if args.Entries is a prefix of rf.Log, 
    // then no conflict occurs, and thus no deletion happens.
    // This avoids an reordered AppendEntries RPCs from the 
    // current leader overwriting the follower's commited entries.
    for i := args.PrevLogIndex + 1; i <= rf.lastLogIndex && len(args.Entries) != 0; i++ {
        if args.Entries[0].Term != rf.Log[i].Term {
            // fmt.Printf("server %v deletes logs after index %v where leader %v, args.PrevLogIndex %v\n", rf.me, i - 1, args.LeaderId, args.PrevLogIndex)
            for k, _ := range rf.Log {
                if k >= i {
                    delete(rf.Log, k)
                }
            }
            rf.lastLogIndex = i - 1
            break
        }
        args.Entries = args.Entries[1:]
    }

    // Append any new entries not already in the log (Figure 2, AppendEntries RPC)
    if len(args.Entries) != 0 {
        // fmt.Printf("server %v appends %v entries starting at index %v\n", rf.me, len(args.Entries), rf.lastLogIndex + 1)
    }
    for _, entry := range args.Entries {
        rf.lastLogIndex += 1
        rf.Log[rf.lastLogIndex] = entry
    }

    // persist ONLY if necessary since we are in a frequently used RPC 
    // and we want to release the lock as soon as possible
    // to avoid lock contention
    if len(args.Entries) != 0 {
        persist = true
    }

    if persist {
        rf.persist(nil)
    }

    // If LeaderCommitIndex > rf.commitIndex, update rf.commitIndex 
    // to min(lastLogIndex, leaderCommitIndex). (Figure 2, AppendEntries RPC)
    if args.LeaderCommitIndex > rf.commitIndex {
        if args.LeaderCommitIndex > rf.lastLogIndex {
            // fmt.Printf("server %v updates commitIndex from %v to %v\n", rf.me, rf.commitIndex, rf.lastLogIndex)
            rf.commitIndex = rf.lastLogIndex
        } else {
            // fmt.Printf("server %v updates commitIndex from %v to %v\n", rf.me, rf.commitIndex, args.LeaderCommitIndex)
            rf.commitIndex = args.LeaderCommitIndex
        }
    }

    reply.Success = true

    // If commitIndex > lastApplied, increment lastApplied and apply 
    // log[lastApplied..=commitIndex] (Figure 2, rules for all servers)
    if rf.commitIndex > rf.lastApplied {
        // fmt.Printf("server %v commitIndex %v > lastapplied index %v\n", rf.me, rf.commitIndex, rf.lastApplied)
        rf.applierCond.Broadcast()
    }

}

//
// Send AppendEntries RPC to a peer.
// Serve as heartsbeats if args.entries == nil.
//
// RPC call can block indefinitely due to network latency, 
// power failure of the receiver and so on. So make sure that 
// the caller does not grab the lock, and remember to 
// check whether the caller's state has changed before
// and after the RPC call if necessary.
// Also make sure that the caller takes the response 
// carefully since RPCs can reorder, lost or duplicate.
//
func (rf *Raft) sendAppendEntries(server int, 
   args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
    ok := rf.peers[server].Call("Raft.AppendEntriesHandler", args, reply)
    return ok
}

//
// Send AppendEntries RPC to all peers.
// If the leader have already discarded the entries that 
// need to be sent to the follower, send InstallSnapshot RPC instead.
//
// When failed, instead of retrying explicitly, we simply do some updates
// (if necessary) and wait for a new RPC to be sent in the next round.
// 
// Raft RPC is designed such that there are only two rules for a single Raft RPC 
// to work "correctly" in the sender's side: 
// 1. The argument should only contain an exact state at some point of a leader, 
// or the RPC can have side effect on the receiver.
// 2. Only the reply that shares the same session with the request is handled,
// or the RPC can have side effect on the sender.
// In fact, AttemptElection() automatically abides this rule when it tries to 
// keep election session consistent between all the requests and all the replies. 
// But SendAppendEntriesAll() can do this far more easily since the RPCs do not 
// need to be sent or handled in the same session.
// 
// To ensure the idempotence of AppendEntries RPC in the sender's side,
// the sender should ignore the reply if rf.nextIndex[server] has changed.
// 
func (rf *Raft) SendAppendEntriesAll() {
    // Unset the election timeout of the leader itself
    rf.unsetTimeout()
    done := 1
    success := 1
    commitCond := sync.NewCond(&rf.mu)

    for server, _ := range rf.peers {
        if server == rf.me {
            continue
        }
        go func(server int) {
            rf.mu.Lock()
            if rf.killed() || rf.state != Leader {
                commitCond.Broadcast()
                rf.mu.Unlock()
                return
            }
            nextIndex := rf.nextIndex[server]
            if nextIndex <= rf.LastIncludedIndex {
                done += 1
                if done == len(rf.peers) {
                    commitCond.Broadcast()
                }
                rf.mu.Unlock()
                go rf.SendInstallSnapshot(server)
                return
            }
            currentTerm := rf.CurrentTerm
            args := AppendEntriesArgs {
                LeaderTerm: rf.CurrentTerm, 
                LeaderId: rf.me,
                PrevLogIndex: nextIndex - 1, 
                PrevLogTerm: rf.Log[nextIndex - 1].Term,
                Entries: log2slice(rf.Log, nextIndex),
                LeaderCommitIndex: rf.commitIndex,
            }
            if len(args.Entries) != 0 {
                // fmt.Printf("server %v sending %v entries at index %v to server %v\n", rf.me, len(args.Entries), nextIndex, server)
            }
            rf.mu.Unlock()

            var reply AppendEntriesReply
            ok := rf.sendAppendEntries(server, &args, &reply)

            rf.mu.Lock()
            defer func() {
                done += 1
                if done == len(rf.peers) {
                    commitCond.Broadcast()
                }
                rf.mu.Unlock()
            }()
            // Ignore the reply if the leader is no longer the leader in this term
            if rf.killed() || rf.state != Leader ||
               rf.CurrentTerm != currentTerm {
                return
            }

            // Ignore the reply if rf.nextIndex[server] has changed
            if rf.nextIndex[server] != nextIndex {
                return
            }

            if !ok {
                return
            }

            if reply.Success {
                if rf.matchIndex[server] != rf.nextIndex[server] + len(args.Entries) - 1 {
                    // fmt.Printf("Server %v updating matchIndex[%v] from %v to %v\n", rf.me, server, rf.matchIndex[server], nextIndex + len(args.Entries) - 1)
                }
                rf.nextIndex[server] += len(args.Entries)
                rf.matchIndex[server] = rf.nextIndex[server] - 1
                success += 1
                if success > len(rf.peers) / 2 {
                    commitCond.Broadcast()
                }
            } else {
                if reply.UpdatedTerm > rf.CurrentTerm {
                    rf.handleHigherTerm(reply.UpdatedTerm, true)
                } else {
                    // If AppendEntries failed due to log inconsistency,
                    // reset nextIndex and retry (Figure 2, rules for leaders)
                    rf.nextIndex[server] = reply.RetryIndex
                }
            }
        }(server)
    }

    rf.mu.Lock()
    defer rf.mu.Unlock()
    for !rf.killed() && rf.state == Leader && done != len(rf.peers) &&
        success <= len(rf.peers) / 2 {
        commitCond.Wait()
    }

    if rf.killed() || rf.state != Leader || success <= len(rf.peers) / 2 {
        return
    }

    // If there exists an N such that N > commitIndex, a majority
    // of matchIndex[i] >= N, and log[N].term == CurrentTerm,
    // set commitIndex = N (Figure 2, rules for leaders)
    // 
    // This means that a leader will never commit an entry from a previous term 
    // **until** there is a newer entry in the current term
    // on a majority of the servers and ready to be committed. Because only at that 
    // time can we claim that after the commit, the entries from the previous will 
    // never get overwritten, and thus are "safe". (§5.4.2)
    for n := rf.lastLogIndex; n > rf.commitIndex; n-- {
        count := 1
        for server := range rf.peers {
            if server == rf.me {
                continue
            }
            if rf.matchIndex[server] >= n {
                count++
            }
        }
        if count > len(rf.peers) / 2 &&
           rf.Log[n].Term == rf.CurrentTerm {
           // fmt.Printf("server %v updates commitIndex from %v to %v\n", rf.me, rf.commitIndex, n)
            rf.commitIndex = n
            break
        }
    }


    // If commitIndex > lastApplied, increment lastApplied and apply 
    // log[lastApplied..=commitIndex] (Figure 2, rules for all servers)
    if rf.commitIndex > rf.lastApplied {
        // fmt.Printf("server %v commitIndex %v > lastapplied index %v\n", rf.me, rf.commitIndex, rf.lastApplied)
        rf.applierCond.Broadcast()
    }
}

func (rf *Raft) readSnapshot() []byte {
    return rf.persister.ReadSnapshot()
}

//
// Legacy API. Simply let it return true.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	return true
}

//
// Called by the tester (state machine) periodically to save a snapshot that 
// has all info of the applied entries up to and including the snapshotIndex.
// All the entries covered by this snapshot will be discarded, and a entry that
// contains the metadata of this snapshot: lastIncludedIndex (the last index of 
// entry covered by this snapshot) and lastIncludedTerm (the term of that entry),
// will be inserted in case of the sanity checks of other RPCs. Since this entry 
// is already in the log, we simply preserve it when discarding the entries. 
// When the next snapshot comes in, this entry will be removed.
// 
// Since the snapshot here should cover only applied entries on this server, and 
// server never overwrites or deletes commited entries, lastCommitIndex and 
// lastApplied will never decrease, thus LastIncludedIndex will never decrease.
//
// Since the snapshot is sent by the tester (state machine), servers don't need to 
// send applyMsg when the snapshot is saved.
//
func (rf *Raft) Snapshot(snapshotIndex int, snapshot []byte) {
    rf.mu.Lock()
    defer rf.mu.Unlock()
    if snapshotIndex <= rf.LastIncludedIndex {
        return
    }
    // fmt.Printf("server %v installing snapshot ending at %v from its state machine\n", rf.me, snapshotIndex)

    // Since index <= rf.lastApplied <= rf.commitIndex <= rf.lastLogIndex,
    // this basically should never happen.
    if snapshotIndex > rf.lastApplied {
        panic("Invalid snapshot")
    }

//    fmt.Printf("server %v takes snapshot from tester ending at index %v\n", rf.me, snapshotIndex)
    for i, _ := range rf.Log {
        if i < snapshotIndex {
            delete(rf.Log, i)
        }
    }
    rf.LastIncludedIndex = snapshotIndex
    rf.persist(snapshot)
}


type InstallSnapshotArgs struct {
    LeaderTerm int
    LeaderId int
    LastIncludedIndex int
    LastIncludedTerm int
    Data []byte
}

type InstallSnapshotReply struct {
    Success bool
    UpdatedTerm int
}


//
// InstallSnapshot RPC Handler.
// 
// Leader sends snapshot to the follower only if the entries 
// it wants to send has been trimed. DO NOT install the snapshot
// if all the entries it covering has been committed by this server.
// In this case, return success to notify the leader that it is safe 
// to update nextIndex[server] and retry AppendEntries RPC.
// 
// Now we know that the snapshot contains at least one entries
// not committed by this server. The snapshot should be install anyway.
// If this server has an entry that does not conflict the last entry
// covered by the snapshot, retain all the entries following it.
// Otherwise discard all the entries.
//
// Move the snapshot to rf.pendingSnapshot before return, which will
// be applied by the applier later. The applier makes sure that
// commands and snapshot won't be applied out of order.
//
// For sanity check, regardless of whether the snapshot will accepted 
// or not, insert an entry at the LastIncludedIndex with LastIncludedTerm
//
// To give an illusion that the entries replaced by the snapshot have 
// been committed and applied:
// - Set rf.lastcommitIndex to args.LastIncludedIndex.
// - Set rf.lastApplied to args.LastIncludedIndex.
// 
func (rf *Raft) InstallSnapshotHandler(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
    rf.mu.Lock()

   // fmt.Printf("server %v received snapshot from server %v ending at %v\n", rf.me, args.LeaderId, args.LastIncludedIndex)

    // reply immediately if args.Term < CurrentTerm (Figure 13, InstallSnapshot RPC)
    if args.LeaderTerm < rf.CurrentTerm {
        reply.Success = false
        reply.UpdatedTerm = rf.CurrentTerm
//    fmt.Printf("server %v rejects snapshot from server %v ending at %v as it has a higher term\n", rf.me, args.LeaderId, args.LastIncludedIndex)
        rf.mu.Unlock()
        return
    }

    // Update CurrentTerm if RPC request contains a higher term
    // (Figure 2, rules for all servers)
    if args.LeaderTerm > rf.CurrentTerm {
        rf.handleHigherTerm(args.LeaderTerm, true)
    }

  // Maintain the authority of the **current** leader (at least this server believes),
    // which is ensured by the above sanity check.
    // The check is not as strict as those of the AppendEntries RPC handler, 
    // as the snapshot can cover only applied entries at some point of a leader.
    rf.unsetTimeout()
    rf.state = Follower

    // Take care that these snapshots only advance the service's state, and don't cause it to move backwards.
    // MAKE SURE to INSERT an entry at the snapshot index in case of check
     if args.LastIncludedIndex <= rf.commitIndex {
         // fmt.Printf("server %v ignores snapshot from server %v since snapshot ending at %v <= commitindex %v\n", rf.me, args.LeaderId, args.LastIncludedIndex, rf.commitIndex)
         reply.Success = true
         if _, ok := rf.Log[args.LastIncludedIndex]; !ok {
            rf.Log[args.LastIncludedIndex] = Entry{Term: args.LastIncludedTerm}
         }
         rf.mu.Unlock()
         return
 
     }

    /* The snapshot contains at least one entries not committed yet */

    // If existing log entry has same index and term as snapshot's last 
    // included entry, retain log entries following it
    // Otherwise, discard the entire logs (Figure 18, InstallSnapshot RPC)
    if _, ok := rf.Log[args.LastIncludedIndex]; ok &&
      rf.Log[args.LastIncludedIndex].Term == args.LastIncludedTerm {
         for i, _ := range rf.Log {
             if i < args.LastIncludedIndex {
                 delete(rf.Log, i)
             }
         }
         // fmt.Printf("server %v retain log after (including) index %v due to snapshot\n", rf.me, args.LastIncludedIndex)
    } else {
        rf.Log = make(map[int]Entry)
        rf.Log[0] = Entry{}
        rf.Log[args.LastIncludedIndex] = Entry{ Term: args.LastIncludedTerm }
        rf.lastLogIndex = args.LastIncludedIndex

        // fmt.Printf("server %v discard the entire log due to snapshot\n", rf.me)
    }

    reply.Success = true
    // fmt.Printf("server %v updating commitIndex to %v due to snapshot\n", rf.me, args.LastIncludedIndex)
    // fmt.Printf("server %v updating lastApplied index to %v due to snapshot\n", rf.me, args.LastIncludedIndex)
    rf.commitIndex = args.LastIncludedIndex
    rf.lastApplied = args.LastIncludedIndex
    rf.LastIncludedIndex = args.LastIncludedIndex
    rf.persist(args.Data)
    rf.pendingSnapshot = args.Data
    rf.applierCond.Broadcast()
    rf.mu.Unlock()
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshotHandler", args, reply)
	return ok
}

//
// Send InstallSnapshot RPC to tell a lagging follower to replace 
// its state with a snapshot. This happens when the leader has 
// already discarded the next log entry that it needs to send
// to that follower.
//
func (rf *Raft) SendInstallSnapshot(server int) {
    rf.mu.Lock()
    if rf.killed() || rf.state != Leader {
        rf.mu.Unlock()
        return
    }
    currentTerm := rf.CurrentTerm
    args := InstallSnapshotArgs {
        LeaderTerm: rf.CurrentTerm,
        LeaderId: rf.me,
        LastIncludedIndex: rf.LastIncludedIndex,
        LastIncludedTerm: rf.Log[rf.LastIncludedIndex].Term,
        Data: rf.readSnapshot(),
    }
    nextIndex := rf.nextIndex[server]
    lastIncludedIndex := rf.LastIncludedIndex
    rf.mu.Unlock()

    var reply InstallSnapshotReply
    ok := rf.sendInstallSnapshot(server, &args, &reply)

    rf.mu.Lock()
    defer rf.mu.Unlock()
    if rf.killed() || rf.state != Leader || rf.CurrentTerm != currentTerm  {
        return
    }

    if nextIndex != rf.nextIndex[server] || lastIncludedIndex != rf.LastIncludedIndex {
        return
    }

    if !ok {
        return
    }

    if reply.Success {
        rf.nextIndex[server] = rf.LastIncludedIndex + 1
        // fmt.Printf("server %v updating matchIndex[%v] from %v to %v due to snapshot\n", rf.me, server, rf.matchIndex[server], rf.LastIncludedIndex)
        rf.matchIndex[server] = rf.LastIncludedIndex
    } else {
        if reply.UpdatedTerm > rf.CurrentTerm {
            rf.handleHigherTerm(reply.UpdatedTerm, true)
        }
    }
}

func (rf *Raft) RaftStatedSize() int {
    return rf.persister.RaftStateSize()
}

// The ticker go routine starts a new election if 
// this peer hasn't received heartsbeats recently.
func (rf *Raft) ticker() {
    rand.Seed(time.Now().UnixNano())
	for !rf.killed() {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using time.Sleep().
        rf.setTimeout()
        time.Sleep(time.Duration(200 + rand.Intn(200)) * time.Millisecond)
        if rf.electionTimeout() {
            go rf.AttemptElection()
        }
	}
}

//
// Send AppendEntries RPC periodically to maintain the authority of leader.
// The frequency should be low enough or it might be hard for the followers 
// to make progress.
//
func (rf *Raft) maintainAuthority() {
    isLeader := func(rf *Raft) bool {
        rf.mu.Lock()
        defer rf.mu.Unlock()
        return rf.state == Leader
    }
    for !rf.killed() {
        // fmt.Printf("server %v ...\n", rf.me)
        go rf.SendAppendEntriesAll()
        time.Sleep(50 * time.Millisecond)
        if !isLeader(rf) {
            break
        }
    }
}

//
// An single applier go routine apply commited entries periodically.
// For each server, there should be only one applier thread in case of 
// reorder apply, since a sever should never grab the lock before passing 
// messages on a channel.
//
// KVServer and Raft Server share on process. 
//
func (rf *Raft) applier() {
    for !rf.killed() {
        rf.mu.Lock()
        for rf.pendingSnapshot == nil && rf.commitIndex <= rf.lastApplied {
            rf.applierCond.Wait()
        }
        //
        // Apply any pending snapshot before we check for 
        // any new commands to be applied.
        // It is fine if we receive a snapshot at rf.pendingSnapshot 
        // after we decide to commit some commands, because all of these
        // commands satisfy commandIndex < snapshotIndex
        //
        if rf.pendingSnapshot != nil {
            msg := ApplyMsg {
                CommandValid: false,
                SnapshotValid: true,
                Snapshot: rf.pendingSnapshot,
                SnapshotIndex: rf.LastIncludedIndex,
                SnapshotTerm: rf.Log[rf.LastIncludedIndex].Term,
            }
            rf.pendingSnapshot = nil
            rf.mu.Unlock()
            rf.applyCh <- msg
            // fmt.Printf("server %v applied snapshot at index %v\n", rf.me, msg.SnapshotIndex)
            continue
        }

        var applyMsgs []ApplyMsg
        for idx := rf.lastApplied + 1; idx <= rf.commitIndex; idx++ {
            applyMsgs = append(applyMsgs, ApplyMsg{
                SnapshotValid: false, 
                CommandValid: true, 
                Command: rf.Log[idx].Command, 
                CommandIndex: idx,
            })
        }
        // fmt.Printf("server %v will apply %v entries at index %v\n", rf.me, len(applyMsgs), applyMsgs[0].CommandIndex) 

        rf.lastApplied = rf.commitIndex
        rf.mu.Unlock()

        // The test will block the channel if it determines to call 
        // rf.Snapshot(), until done. In this time, no additional apply
        // will make progress.
        for _, msg := range applyMsgs {
            rf.applyCh <- msg
            // fmt.Printf("server %v applied entry at index %v (may delayed)\n", rf.me, msg.CommandIndex) 
        }
        if len(applyMsgs) != 0 {
            // fmt.Printf("server %v applied %v entries at index %v (may delayed)\n", rf.me, len(applyMsgs), applyMsgs[0].CommandIndex) 
        }
    }
}


//
// The service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// The first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
// 
func (rf *Raft) Start(command interface{}) (int, int, bool) {
    rf.mu.Lock()
    defer rf.mu.Unlock()
    if rf.killed() || rf.state != Leader {
        return -1, -1, false
    }

    // Replicate the entry on the leader's own log
    entry := Entry{ Command: command, Term: rf.CurrentTerm }
    rf.lastLogIndex += 1
    rf.Log[rf.lastLogIndex] = entry
    rf.persist(nil)
    // fmt.Printf("server %v replicates one entry on its own log at index %v\n", rf.me, rf.lastLogIndex)

    // send AppendEntries RPC immediately to reduce the per-op latency
    go rf.SendAppendEntriesAll()

    return rf.lastLogIndex, rf.CurrentTerm, true
}

//
// The tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// The issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

//
// The service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
    rf.applyCh = applyCh
    rf.applierCond = sync.NewCond(&rf.mu)
    rf.state = Follower

	// initialize from state persisted before a crash
    state := persister.ReadRaftState()
    if len(state) == 0 {
        rf.Log = make(map[int]Entry)
        // log[0] is trivial
        rf.Log[0] = Entry{}
        rf.VotedFor = -1
        rf.persist(nil)
    } else {
	    rf.readPersist(state)
         // fmt.Printf("server %v reboots\n", rf.me)
        // Restores rf.lastLogIndex
        for i, _ := range rf.Log {
            if i > rf.lastLogIndex {
                rf.lastLogIndex = i
            }
        }
        // set rf.commitIndex and rf.lastApplied to 
        // rf.LastIncludedIndex instead of 0, since the entries covered by 
        // snapshot don't need to be commited or applied again.
        rf.commitIndex = rf.LastIncludedIndex
        rf.lastApplied = rf.LastIncludedIndex

        if rf.LastIncludedIndex > 0 {
            rf.pendingSnapshot = rf.readSnapshot()
        }
    }

	// start ticker goroutine to start elections
	go rf.ticker()
    go rf.applier()

	return rf
}
