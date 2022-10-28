package raft

//
// this is an outline of the API that raft must expose to
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
	//	"bytes"

	"bytes"
	"errors"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labgob"
	"6.824/labrpc"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
	CommandTerm  int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

const (
	FOLLOWER = iota
	CANDIDATE
	LEADER
)

const (
	NONE = iota
	NEW_ELECTION
)

const TIME_STEP = 10

const DEBUG = 1

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
	state int

	currentTerm int
	votedFor    int
	log         []ApplyMsg

	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int

	lastElectionTime time.Time
	electionTimeout  int // millisecond
	timeoutBase      int
	timeoutOffset    int

	lastHeartbeatTime time.Time
	heartbeatTimeout  int

	cond       *sync.Cond
	condLock   sync.Mutex
	wakeUpType int

	voteCount int

	applyCh chan ApplyMsg
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = (rf.state == LEADER)
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)

	// Must be called with rf.mu held
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	DPrintf("%v: readPersist func start\n", rf.me)
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }

	// Must be call with rf.mu held
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var logs []ApplyMsg
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&logs) != nil {
		log.Fatal(errors.New("persister decoding error"))
	} else {
		DPrintf("%v: readPersist success:\n", rf.me)
		DPrintf("\tcurrentTerm: %v\n", currentTerm)
		DPrintf("\tvotedFor: %v\n", votedFor)
		DPrintf("\tlog: %v\n", logs)
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = rf.log[:0]
		rf.log = append(rf.log, logs...)
		DPrintf("\trf.currentTerm: %v\n", rf.currentTerm)
		DPrintf("\trf.votedFor: %v\n", rf.votedFor)
		DPrintf("\trf.log: %v\n", rf.log)
		DPrintf("\trf.state: %v\n", rf.state)
	}
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	DPrintf("%v: receive request vote from %v, term %v, curState %v, curVoteFor %v\n", rf.me, args.CandidateId, args.Term, rf.state, rf.votedFor)
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
	}
	if args.Term < rf.currentTerm {
		rf.persist()
		*reply = RequestVoteReply{Term: rf.currentTerm, VoteGranted: false}
		return
	}
	myLastIndex := rf.lastLog().CommandIndex
	myLastTerm := rf.lastLog().CommandTerm
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		if (args.LastLogTerm > myLastTerm) || ((args.LastLogTerm == myLastTerm) && (args.LastLogIndex >= myLastIndex)) {
			rf.votedFor = args.CandidateId
			rf.resetTicker()
			rf.persist()
			*reply = RequestVoteReply{Term: rf.currentTerm, VoteGranted: true}
			DPrintf("%v: vote to %v, term %v\n", rf.me, rf.votedFor, rf.currentTerm)
			return
		}
	}
	rf.persist()
	*reply = RequestVoteReply{Term: rf.currentTerm, VoteGranted: false}
	DPrintf("%v: didn't vote\n", rf.me)
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

type AppendEntriesArgs struct {
	// capital
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []ApplyMsg
	LeaderCommit int
}

type AppendEntriesReply struct {
	// capital
	Term    int
	Success bool
}

func min(a int, b int) int {
	if a < b {
		return a
	}
	return b
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	DPrintf("%v current logs: %v\n", rf.me, rf.log)
	defer rf.mu.Unlock()

	//rf.resetTicker()
	if args.Term < rf.currentTerm {
		rf.persist()
		*reply = AppendEntriesReply{Term: rf.currentTerm, Success: false}
		DPrintf("%v: reject appendEntries, term %v, myterm %v\n", rf.me, args.Term, rf.currentTerm)
		return
	}

	if args.Term > rf.currentTerm || rf.state == CANDIDATE {
		rf.becomeFollower(args.Term)
	}
	rf.resetTicker()
	matchLogPos := -1
	for i, log := range rf.log {
		if log.CommandIndex == args.PrevLogIndex && log.CommandTerm == args.PrevLogTerm {
			matchLogPos = i
			break
		}
	}
	if matchLogPos == -1 {
		rf.persist()
		*reply = AppendEntriesReply{Term: rf.currentTerm, Success: false}
		DPrintf("%v: reject appendEntries, prevlogindex %v, no match index\n", rf.me, args.PrevLogIndex)
		return
	}
	if args.Entries != nil {
		// rf.log = rf.log[:matchLogPos+1]
		// rf.log = append(rf.log, args.Entries...)
		i := matchLogPos + 1
		j := 0
		for i < len(rf.log) && j < len(args.Entries) {
			if (rf.log[i].CommandIndex != args.Entries[j].CommandIndex) || (rf.log[i].CommandTerm != args.Entries[j].CommandTerm) {
				break
			}
			i++
			j++
		}
		if j != len(rf.log) {
			if i != len(rf.log) {
				rf.log = rf.log[:i]
			}
			rf.log = append(rf.log, args.Entries[j:]...)
		}
	}
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, rf.lastLog().CommandIndex)
	}
	//rf.resetTicker()
	rf.persist()
	*reply = AppendEntriesReply{Term: rf.currentTerm, Success: true}
	if args.Entries == nil {
		DPrintf("%v: receive heartbeat, term %v\n", rf.me, args.Term)
	} else {
		DPrintf("%v: receive appendEntries, term %v, index %v\n", rf.me, args.Term, args.Entries[0].CommandIndex)
	}

}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := false

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state == LEADER {
		index = rf.lastLog().CommandIndex + 1
		term = rf.currentTerm
		isLeader = true
		log := ApplyMsg{Command: command, CommandIndex: index, CommandTerm: term}
		rf.log = append(rf.log, log)
		rf.persist()
		rf.nextIndex[rf.me] = index + 1
		rf.matchIndex[rf.me] = index
		DPrintf("%v: get command, index %v\n", rf.me, log.CommandIndex)
	}

	return index, term, isLeader
}

func (rf *Raft) lastLog() ApplyMsg {
	return rf.log[len(rf.log)-1]
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
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

// Must be called with rf.mu held
func (rf *Raft) resetTicker() {
	rf.lastElectionTime = time.Now()
	rf.electionTimeout = rf.timeoutBase + rand.Intn(rf.timeoutOffset)
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		time.Sleep(TIME_STEP * time.Millisecond)

		rf.mu.Lock()
		//DPrintf("ticker %v get rf.mu\n", rf.me)
		duration := time.Since(rf.lastElectionTime)
		if duration.Milliseconds() < int64(rf.electionTimeout) {
			//DPrintf("ticker %v release rf.mu\n", rf.me)
			rf.mu.Unlock()
			continue
		}

		if rf.state != LEADER {
			DPrintf("%v: ticker timeout, becoming candidate, term %v\n", rf.me, rf.currentTerm+1)
			// rule follower 2
			rf.condLock.Lock()
			rf.wakeUpType = NEW_ELECTION
			rf.condLock.Unlock()
			rf.cond.Broadcast()
		}
		//DPrintf("ticker %v release rf.mu\n", rf.me)
		rf.mu.Unlock()
	}
}

func (rf *Raft) heartbeat() {
	for rf.killed() == false {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		time.Sleep(TIME_STEP * time.Millisecond)

		rf.mu.Lock()
		duration := time.Since(rf.lastHeartbeatTime)
		if duration.Milliseconds() < int64(rf.heartbeatTimeout) {
			rf.mu.Unlock()
			continue
		}
		if rf.state != LEADER {
			rf.mu.Unlock()
			continue
		}

		rf.sendHeartbeat()
		DPrintf("%v: send heartbeat to all, term %v\n", rf.me, rf.currentTerm)

		rf.lastHeartbeatTime = time.Now()
		rf.mu.Unlock()
	}
}

// Must be call with rf.mu held
func (rf *Raft) sendHeartbeat() {
	//term := rf.currentTerm
	args_tmp := AppendEntriesArgs{Term: rf.currentTerm, LeaderId: rf.me, PrevLogIndex: rf.lastLog().CommandIndex, PrevLogTerm: rf.lastLog().CommandTerm, LeaderCommit: rf.commitIndex}
	reply_tmp := AppendEntriesReply{}
	var wg sync.WaitGroup
	wg.Add(len(rf.peers) - 1)
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(i int) {
			defer wg.Done()
			DPrintf("%v: send heartbeat to %v\n", rf.me, i)
			args := &AppendEntriesArgs{}
			reply := &AppendEntriesReply{}
			*args = args_tmp
			*reply = reply_tmp
			ok := rf.sendAppendEntries(i, args, reply)

			// if term > rf.currentTerm || rf.state != LEADER || !ok {
			// 	return
			// }
			// if reply.Term > rf.currentTerm {
			// 	rf.becomeFollower(reply.Term)
			// }

			if rf.state != LEADER {
				return
			}
			if !ok {
				DPrintf("%v: send AppendEntries to %v not OK\n", rf.me, i)
				return
			}
			if reply.Term > rf.currentTerm {
				rf.becomeFollower(reply.Term)
				DPrintf("%v: send AppendEntries fail, become follower term %v\n", rf.me, reply.Term)
				return
			}
			if !reply.Success {
				if rf.nextIndex[i] > 1 {
					rf.nextIndex[i]--
				}

				DPrintf("%v: send AppendEntries to %v not success, nextIndex[%v]==%v\n", rf.me, i, i, rf.nextIndex[i])
			}
		}(i)
	}
}

func (rf *Raft) newElection() {
	for rf.killed() == false {
		//DPrintf("new election in %v", rf.me)
		rf.condLock.Lock()
		//DPrintf("new election get lock %v", rf.me)
		for rf.wakeUpType != NEW_ELECTION {
			//DPrintf("new election wait %v", rf.me)
			rf.cond.Wait()
		}
		rf.wakeUpType = NONE
		rf.condLock.Unlock()

		DPrintf("%v: candidate wakeup, term %v\n", rf.me, rf.currentTerm+1)

		rf.mu.Lock()
		//DPrintf("candidate wakeup %v get rf.mu\n", rf.me)
		rf.state = CANDIDATE
		rf.currentTerm++
		rf.voteCount = 1
		rf.votedFor = rf.me
		rf.persist()
		rf.resetTicker()

		term := rf.currentTerm
		args_tmp := RequestVoteArgs{Term: rf.currentTerm, CandidateId: rf.me, LastLogIndex: rf.lastLog().CommandIndex, LastLogTerm: rf.lastLog().CommandTerm}
		var wg sync.WaitGroup
		wg.Add(len(rf.peers) - 1)
		for i := range rf.peers {
			if i == rf.me {
				continue
			}
			go func(i int) {
				defer wg.Done()
				DPrintf("%v: send requestvote to %v, term %v\n", rf.me, i, rf.currentTerm)
				args := &RequestVoteArgs{}
				*args = args_tmp
				reply := &RequestVoteReply{}
				ok := rf.sendRequestVote(i, args, reply)
				DPrintf("%v: send requestvote to %v return, return value %v, term %v\n", rf.me, i, ok, reply.Term)
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if rf.currentTerm != term || rf.state != CANDIDATE || !ok {
					return
				}
				if reply.Term > rf.currentTerm {
					rf.becomeFollower(reply.Term)
					return
				}
				if reply.VoteGranted {
					DPrintf("%v: receive valid vote from %v, term %v\n", rf.me, i, reply.Term)
					rf.voteCount++
				}
				if rf.voteCount > 1 && rf.voteCount > len(rf.peers)/2 {
					DPrintf("%v: become leader, term %v\n", rf.me, rf.currentTerm)
					rf.becomeLeader()
				}
			}(i)
		}

		rf.mu.Unlock()
	}
}

// Must be called with rf.mu held
func (rf *Raft) becomeLeader() {
	rf.state = LEADER
	rf.lastHeartbeatTime = time.Now()
	for i := range rf.nextIndex {
		rf.nextIndex[i] = rf.lastLog().CommandIndex + 1
	}
	for i := range rf.matchIndex {
		if i == rf.me {
			rf.matchIndex[i] = rf.lastLog().CommandIndex
		} else {
			rf.matchIndex[i] = 0
		}
	}
	rf.sendHeartbeat()
}

// Must be called with rf.mu held
func (rf *Raft) becomeFollower(term int) {
	rf.state = FOLLOWER
	rf.currentTerm = term
	rf.votedFor = -1
	rf.persist()
	rf.resetTicker()
}

func (rf *Raft) commitLogs() {
	for rf.killed() == false {
		time.Sleep(TIME_STEP * time.Millisecond)

		rf.mu.Lock()
		if rf.state != LEADER {
			rf.mu.Unlock()
			continue
		}

		var wg sync.WaitGroup
		wg.Add(len(rf.peers) - 1)
		for i := range rf.peers {
			if i == rf.me {
				continue
			}
			DPrintf("%v: commitLogs loop i:%v\n", rf.me, i)
			go func(i int) {
				defer wg.Done()
				for rf.killed() == false {
					rf.mu.Lock()
					if rf.state != LEADER {
						rf.mu.Unlock()
						return
					}
					DPrintf("%v: commitLogs i:%v nextIndex:%v\n", rf.me, i, rf.nextIndex[i])
					if rf.nextIndex[i] > rf.lastLog().CommandIndex {
						rf.mu.Unlock()
						return
					}

					pos := rf.getPosByIndex(rf.nextIndex[i])

					args := &AppendEntriesArgs{Term: rf.currentTerm, LeaderId: rf.me, PrevLogIndex: rf.log[pos-1].CommandIndex, PrevLogTerm: rf.log[pos-1].CommandTerm, Entries: rf.log[pos:], LeaderCommit: rf.commitIndex}
					reply := &AppendEntriesReply{}
					rf.mu.Unlock()
					DPrintf("%v: send AppendEntries to %v, entries %v\n", rf.me, i, args.Entries)
					DPrintf("%v current logs: %v\n", rf.me, rf.log)
					ok := rf.sendAppendEntries(i, args, reply)
					rf.mu.Lock()

					if rf.state != LEADER {
						rf.mu.Unlock()
						return
					}
					if !ok {
						DPrintf("%v: send AppendEntries to %v not OK\n", rf.me, i)
						rf.mu.Unlock()
						return
					}
					if reply.Term > rf.currentTerm {
						rf.becomeFollower(reply.Term)
						DPrintf("%v: send AppendEntries fail, become follower term %v\n", rf.me, reply.Term)
						rf.mu.Unlock()
						return
					}
					if reply.Success {
						rf.nextIndex[i] = args.PrevLogIndex + len(args.Entries) + 1
						rf.matchIndex[i] = rf.nextIndex[i] - 1
						DPrintf("%v: send AppendEntries to %v success, nextIndex[%v]==%v\n", rf.me, i, i, rf.nextIndex[i])
						rf.mu.Unlock()
						return
					} else {
						if rf.nextIndex[i] > 1 {
							rf.nextIndex[i]--
						}

						DPrintf("%v: send AppendEntries to %v not success, nextIndex[%v]==%v\n", rf.me, i, i, rf.nextIndex[i])
					}
					rf.mu.Unlock()
				}
			}(i)
		}

		rf.mu.Unlock()
	}
}

func (rf *Raft) applyLogs() {
	for rf.killed() == false {
		time.Sleep(TIME_STEP * time.Millisecond)

		rf.mu.Lock()
		for rf.commitIndex > rf.lastApplied {
			rf.lastApplied++

			pos := rf.getPosByIndex(rf.lastApplied)
			log := rf.log[pos]

			log.CommandValid = true
			rf.applyCh <- log
			DPrintf("%v: apply log, index %v\n", rf.me, log.CommandIndex)
		}

		rf.mu.Unlock()
	}
}

func (rf *Raft) updateCommitIndex() {
	for rf.killed() == false {
		time.Sleep(TIME_STEP * time.Millisecond)

		rf.mu.Lock()

		if rf.state != LEADER {
			rf.mu.Unlock()
			continue
		}

		for i := 0; i < len(rf.log); i++ {
			if rf.log[i].CommandTerm != rf.currentTerm {
				continue
			}

			N := rf.log[i].CommandIndex
			if N <= rf.commitIndex {
				continue
			}

			count := 0
			for j := range rf.matchIndex {
				if rf.matchIndex[j] >= N {
					count++
				}
			}

			if count > len(rf.peers)/2 {
				rf.commitIndex = N
				DPrintf("%v: commitIndex updated to %v\n", rf.me, rf.commitIndex)
			}
		}

		rf.mu.Unlock()
	}
}

func (rf *Raft) getPosByIndex(index int) int {
	pos := -1
	for i := range rf.log {
		if rf.log[i].CommandIndex == index {
			pos = i
			break
		}
	}
	return pos
}

//
// the service or tester wants to create a Raft server. the ports
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
	DPrintf("%v: make\n", rf.me)

	// Your initialization code here (2A, 2B, 2C).
	rf.mu.Lock()
	DPrintf("%v: make get lock\n", rf.me)
	rf.state = FOLLOWER
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = append(rf.log, ApplyMsg{}) // empty log
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.lastElectionTime = time.Now()
	rf.timeoutBase = 300
	rf.timeoutOffset = 150
	rf.electionTimeout = rf.timeoutBase + rand.Intn(rf.timeoutOffset)
	rf.lastHeartbeatTime = time.Now()
	rf.heartbeatTimeout = 50
	rf.applyCh = applyCh

	rf.cond = sync.NewCond(&rf.condLock)
	rf.wakeUpType = NONE

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	rf.mu.Unlock()

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.heartbeat()
	go rf.newElection()
	go rf.commitLogs()
	go rf.applyLogs()
	go rf.updateCommitIndex()

	DPrintf("%v: started\n", rf.me)

	return rf
}
