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
	"fmt"
	"log"
	"math/rand"
	"os"
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

const TIME_STEP = 1

const CHUNK_SIZE = 2147483647

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

	snapshot       []byte
	snapshotBuffer []byte
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	DPrintf2("%v: get mutex at GetState\n", rf.me)
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = (rf.state == LEADER)
	DPrintf2("%v: release mutex at GetState\n", rf.me)
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
	DPrintf4("%v: persist, len(data):%v, len(log):%v\n", rf.me, len(data), len(rf.log))
	rf.persister.SaveStateAndSnapshot(data, rf.snapshot)
	DPrintf("%v: persist, currentTerm==%v, votedFor==%v, log==%v, snapshot==%v\n", rf.me, rf.currentTerm, rf.votedFor, rf.log, rf.snapshot)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
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
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = rf.log[:0]
		rf.log = append(rf.log, logs...)
		rf.lastApplied = rf.log[0].CommandIndex
		rf.commitIndex = rf.log[0].CommandIndex
		DPrintf("%v: readPersist success, rf.currentTerm:%v, rf.votedFor:%v, rf.log:%v, rf.state:%v\n", rf.me, rf.currentTerm, rf.votedFor, rf.log, rf.state)
	}
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
// follower想要应用从applyCh获得的快照，此时需要向raft检查这次快照之后有没有新的快照
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf4("%v: CondInstallSnapshot, currentTerm==%v, rf.lastIncludedIndex==%v, rf.lastIncludedTerm==%v, lastIncludedIndex==%v, lastIncludedTerm==%v\n", rf.me, rf.currentTerm, rf.log[0].CommandIndex, rf.log[0].CommandTerm, lastIncludedIndex, lastIncludedTerm)

	if lastIncludedIndex <= rf.commitIndex {
		DPrintf4("%v: condinstallsnapshot fail\n", rf.me)
		return false
	}

	before := len(rf.log)

	rf.snapshot = snapshot
	if lastIncludedIndex > rf.lastLogIndex() {
		rf.log = rf.log[:1]
		rf.log[0].CommandIndex = lastIncludedIndex
		rf.log[0].CommandTerm = lastIncludedTerm
		DPrintf4("%v: condinstallsnapshot rpc done, cut whole log, log==%v\n", rf.me, rf.log)
	} else {
		matchPos := -1
		for i := 0; i < len(rf.log); i++ {
			if rf.log[i].CommandIndex == lastIncludedIndex {
				matchPos = i
				break
			}
		}

		if matchPos != -1 {
			rf.log = append([]ApplyMsg{rf.log[0]}, rf.log[matchPos+1:]...)
			rf.log[0].CommandIndex = lastIncludedIndex
			rf.log[0].CommandTerm = lastIncludedTerm
			DPrintf4("%v: condinstallsnapshot rpc done, cut log, log==%v\n", rf.me, rf.log)
		} else {
			fmt.Printf("matchPos == -1")
			os.Exit(1)
		}
	}

	// if (rf.log[0].CommandTerm == 0 && rf.log[0].CommandIndex == 0) || lastIncludedTerm > rf.log[0].CommandTerm || (lastIncludedTerm == rf.log[0].CommandTerm && lastIncludedIndex > rf.log[0].CommandTerm) {
	// 	rf.snapshot = snapshot

	// 	if lastIncludedIndex > rf.lastLogIndex() {
	// 		rf.log = rf.log[:1]
	// 		rf.log[0].CommandIndex = lastIncludedIndex
	// 		rf.log[0].CommandTerm = lastIncludedTerm
	// 		DPrintf3("%v: condinstallsnapshot rpc done, cut whole log, log==%v\n", rf.me, rf.log)
	// 	} else {
	// 		matchPos := -1
	// 		for i := 0; i < len(rf.log); i++ {
	// 			if rf.log[i].CommandIndex == lastIncludedIndex {
	// 				matchPos = i
	// 				break
	// 			}
	// 		}

	// 		if matchPos != -1 {
	// 			rf.log = append([]ApplyMsg{rf.log[0]}, rf.log[matchPos+1:]...)
	// 			rf.log[0].CommandIndex = lastIncludedIndex
	// 			rf.log[0].CommandTerm = lastIncludedTerm
	// 			DPrintf3("%v: condinstallsnapshot rpc done, cut log, log==%v\n", rf.me, rf.log)
	// 		} else {
	// 			fmt.Printf("matchPos == -1")
	// 			os.Exit(1)
	// 		}
	// 	}

	rf.commitIndex = lastIncludedIndex
	rf.lastApplied = lastIncludedIndex
	rf.persist()

	DPrintf3("%v: condinstallsnapshot rpc done, log==%v\n", rf.me, rf.log)
	after := len(rf.log)
	DPrintf4("%v: condinstallsnapshot before size:%v, after size:%v\n", rf.me, before, after)

	return true

	// 	return true
	// } else {
	// 	return false
	// }
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	DPrintf4("%v: snapshot index:%v\n", rf.me, index)
	// Your code here (2D).
	rf.mu.Lock()
	DPrintf4("%v: get mutex at Snapshot\n", rf.me)
	defer rf.mu.Unlock()

	if index <= rf.log[0].CommandIndex {
		DPrintf("%v: snapshot fail, index %v too small", rf.me, index)
		DPrintf4("%v: snapshot fail, index %v too small", rf.me, index)
		return
	}

	before := len(rf.log)

	pos := rf.getPosByIndex(index)
	if pos != -1 {
		rf.log[0].CommandIndex = rf.log[pos].CommandIndex
		rf.log[0].CommandTerm = rf.log[pos].CommandTerm
		rf.log = append([]ApplyMsg{rf.log[0]}, rf.log[pos+1:]...)
		rf.snapshot = snapshot
		rf.persist()
		DPrintf("%v: snapshot complete, index==%v, rf.currentTerm==%v, rf.lastIncludedIndex==%v, rf.lastIncludedTerm==%v, snapshot==%v\n", rf.me, index, rf.currentTerm, rf.log[0].CommandIndex, rf.log[0].CommandTerm, snapshot)
	} else {
		fmt.Printf("%v: snapshot fail, index %v not found", rf.me, index)
		os.Exit(1)
	}
	DPrintf2("%v: release mutex at Snapshot\n", rf.me)

	after := len(rf.log)

	DPrintf4("%v: snapshot before size:%v, after size:%v\n", rf.me, before, after)
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
	DPrintf("%v: processing requestvote rpc, args:%v\n", rf.me, args)
	// Your code here (2A, 2B).
	rf.mu.Lock()
	DPrintf2("%v: get mutex at RequestVote\n", rf.me)
	defer rf.mu.Unlock()
	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
	}
	if args.Term < rf.currentTerm {
		// rf.persist()
		DPrintf("%v: reject vote, term too small, currentTerm==%v, args.Term==%v\n", rf.me, rf.currentTerm, args.Term)
		*reply = RequestVoteReply{Term: rf.currentTerm, VoteGranted: false}
		DPrintf2("%v: release mutex at RequestVote\n", rf.me)
		return
	}
	myLastIndex := rf.lastLogIndex()
	myLastTerm := rf.lastLogTerm()
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		if (args.LastLogTerm > myLastTerm) || ((args.LastLogTerm == myLastTerm) && (args.LastLogIndex >= myLastIndex)) {
			rf.votedFor = args.CandidateId
			rf.resetTicker()
			rf.persist()
			*reply = RequestVoteReply{Term: rf.currentTerm, VoteGranted: true}
			DPrintf("%v: vote to %v, term %v\n", rf.me, rf.votedFor, rf.currentTerm)
			DPrintf2("%v: release mutex at RequestVote\n", rf.me)
			return
		}
	}
	// rf.persist()
	*reply = RequestVoteReply{Term: rf.currentTerm, VoteGranted: false}
	DPrintf("%v: reject vote, has voted or lastlog not match, votedFor==%v, args.LastLogTerm==%v, myLastTerm==%v, args.LastLogIndex==%v, myLastIndex==%v\n", rf.me, rf.votedFor, args.LastLogTerm, myLastTerm, args.LastLogIndex, myLastIndex)
	DPrintf2("%v: release mutex at RequestVote\n", rf.me)
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
	Term          int
	Success       bool
	ConflictIndex int
	ConflictTerm  int
}

func min(a int, b int) int {
	if a < b {
		return a
	}
	return b
}

func max(a int, b int) int {
	if a > b {
		return a
	}
	return b
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	DPrintf("%v: processing appendentries rpc, args:%v\n", rf.me, args)
	rf.mu.Lock()
	DPrintf2("%v: get mutex at AppendEntries\n", rf.me)
	defer rf.mu.Unlock()

	//rf.resetTicker()
	if args.Term < rf.currentTerm {
		// rf.persist()
		*reply = AppendEntriesReply{Term: rf.currentTerm, Success: false}
		DPrintf("%v: reject appendEntries, term too small, term %v, myterm %v\n", rf.me, args.Term, rf.currentTerm)
		DPrintf2("%v: release mutex at AppendEntries\n", rf.me)
		return
	}

	if args.Term > rf.currentTerm || rf.state == CANDIDATE {
		rf.becomeFollower(args.Term)
	}
	rf.resetTicker()

	matchLogPos := -1
	for i, log := range rf.log {
		if log.CommandIndex == args.PrevLogIndex {
			matchLogPos = i
			break
		}
	}

	if rf.lastLogIndex() < args.PrevLogIndex {
		// rf.persist()
		*reply = AppendEntriesReply{Term: rf.currentTerm, Success: false, ConflictIndex: rf.lastLogIndex(), ConflictTerm: 0}
		DPrintf("%v: reject appendEntries, prevlogindex %v, no match index, log==%v\n", rf.me, args.PrevLogIndex, rf.log)
		DPrintf2("%v: release mutex at AppendEntries\n", rf.me)
		return
	} else if matchLogPos == -1 {
		*reply = AppendEntriesReply{Term: rf.currentTerm, Success: false, ConflictIndex: rf.log[0].CommandIndex, ConflictTerm: rf.log[0].CommandTerm}
		DPrintf("%v: reject appendEntries, prevlogindex %v, no match index, log==%v\n", rf.me, args.PrevLogIndex, rf.log)
		DPrintf2("%v: release mutex at AppendEntries\n", rf.me)
		return
	} else if rf.log[matchLogPos].CommandTerm != args.PrevLogTerm {
		conflictTerm := rf.log[matchLogPos].CommandTerm
		conflictIndex := 0
		for _, log := range rf.log {
			if log.CommandTerm == conflictTerm {
				conflictIndex = log.CommandIndex
				break
			}
		}
		*reply = AppendEntriesReply{Term: rf.currentTerm, Success: false, ConflictIndex: conflictIndex, ConflictTerm: conflictTerm}
		DPrintf("%v: reject appendEntries, prevlogindex %v, no match index, log==%v\n", rf.me, args.PrevLogIndex, rf.log)
		DPrintf2("%v: release mutex at AppendEntries\n", rf.me)
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
		if i < len(rf.log) && j < len(args.Entries) && rf.log[i].CommandIndex != args.Entries[j].CommandIndex {
			fmt.Printf("rf.log[i].CommandIndex != args.Entries[j].CommandIndex")
			os.Exit(1)
		}
		if j != len(args.Entries) {
			if i != len(rf.log) {
				rf.log = rf.log[:i]
			}
			rf.log = append(rf.log, args.Entries[j:]...)
		}
	}
	rf.persist()
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, rf.lastLogIndex())
		DPrintf("%v: update commitindex to %v\n", rf.me, rf.commitIndex)
	}
	//rf.resetTicker()
	*reply = AppendEntriesReply{Term: rf.currentTerm, Success: true}
	if args.Entries == nil {
		DPrintf("%v: receive heartbeat success, term %v\n", rf.me, args.Term)
	} else {
		DPrintf("%v: receive appendEntries success, log==%v\n", rf.me, rf.log)
	}
	DPrintf2("%v: release mutex at AppendEntries\n", rf.me)
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

type InstallSnapshotArgs struct {
	// capital
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Offset            int
	Data              []byte
	Done              bool
}

type InstallSnapshotReply struct {
	// capital
	Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	DPrintf4("%v: processing installsnapshot rpc, args:%v\n", rf.me, args)
	rf.mu.Lock()
	DPrintf4("%v: get mutex at InstallSnapshot\n", rf.me)

	*reply = InstallSnapshotReply{Term: max(rf.currentTerm, args.Term)}

	if args.Term < rf.currentTerm {
		DPrintf4("%v: installsnapshot rpc reject due to term, args.Term==%v, rf.currentTerm==%v\n", rf.me, args.Term, rf.currentTerm)
		rf.mu.Unlock()
		DPrintf2("%v: release mutex at InstallSnapshot\n", rf.me)
		return
	}

	if args.Term > rf.currentTerm {
		DPrintf4("%v: installsnapshot rpc becomeFollower, args.Term==%v, rf.currentTerm==%v\n", rf.me, args.Term, rf.currentTerm)
		rf.becomeFollower(args.Term)
	}

	if args.Offset > 0 || !args.Done {
		fmt.Printf("snapshot chunk not implemented")
		os.Exit(1)
	}

	// if args.Offset == 0 {
	// 	rf.snapshotBuffer = rf.snapshotBuffer[:0]
	// }
	// rf.snapshotBuffer = append(rf.snapshotBuffer[:args.Offset], args.Data...)
	// if !args.Done {
	// 	DPrintf("%v: installsnapshot rpc return but not done, snapshotBuffer==%v\n", rf.me, rf.snapshotBuffer)
	// 	rf.mu.Unlock()
	// 	DPrintf2("%v: release mutex at InstallSnapshot\n", rf.me)
	// 	return
	// }
	// if len(rf.snapshot) != 0 {
	// 	r := bytes.NewBuffer(rf.snapshot)
	// 	d := labgob.NewDecoder(r)
	// 	var oldIndex int
	// 	if d.Decode(&oldIndex) != nil {
	// 		log.Fatal(errors.New("InstallSnapshot decoding error"))
	// 	}
	// 	if oldIndex < args.LastIncludedIndex {
	// 		rf.snapshot = rf.snapshotBuffer
	// 		rf.persist()
	// 		DPrintf("%v: installsnapshot rpc snapshot modified\n", rf.me)
	// 	}
	// } else {
	// 	rf.snapshot = rf.snapshotBuffer
	// 	rf.persist()
	// 	DPrintf("%v: installsnapshot rpc snapshot modified\n", rf.me)
	// }

	// if rf.log[0].CommandIndex < args.LastIncludedIndex {
	// 	rf.snapshot = rf.snapshotBuffer
	// 	// modify snapshot but not modify rf.log[0].CommandIndex, do not persist()
	// 	DPrintf("%v: installsnapshot rpc snapshot modified\n", rf.me)
	// }
	if args.LastIncludedIndex <= rf.log[0].CommandIndex {
		DPrintf4("%v: installsnapshot rpc done, args.LastIncludedIndex <= rf.log[0].CommandIndex, log==%v\n", rf.me, rf.log)
		rf.mu.Unlock()
		DPrintf2("%v: release mutex at InstallSnapshot\n", rf.me)
		return
	}

	applyMsg := ApplyMsg{SnapshotValid: true, Snapshot: args.Data, SnapshotTerm: args.LastIncludedTerm, SnapshotIndex: args.LastIncludedIndex}
	rf.applyCh <- applyMsg // for lab3 test ?

	rf.mu.Unlock()
	DPrintf4("%v: release mutex at InstallSnapshot\n", rf.me)

	// 	rf.applyCh <- applyMsg // for lab2 test
	DPrintf4("%v: installsnapshot rpc apply:%v\n", rf.me, applyMsg)

	// rf.snapshot = args.Data

	// if args.LastIncludedIndex > rf.lastLogIndex() {
	// 	rf.log = rf.log[:1]
	// 	rf.log[0].CommandIndex = args.LastIncludedIndex
	// 	rf.log[0].CommandTerm = args.LastIncludedTerm
	// 	DPrintf("%v: installsnapshot rpc done, cut whole log, log==%v\n", rf.me, rf.log)
	// } else {
	// 	matchPos := -1
	// 	for i := 0; i < len(rf.log); i++ {
	// 		if rf.log[i].CommandIndex == args.LastIncludedIndex {
	// 			matchPos = i
	// 			break
	// 		}
	// 	}

	// 	if matchPos != -1 {
	// 		rf.log = append([]ApplyMsg{rf.log[0]}, rf.log[matchPos+1:]...)
	// 		rf.log[0].CommandIndex = args.LastIncludedIndex
	// 		rf.log[0].CommandTerm = args.LastIncludedTerm
	// 		DPrintf("%v: installsnapshot rpc done, cut log, log==%v\n", rf.me, rf.log)
	// 	} else {
	// 		fmt.Printf("matchPos == -1")
	// 		os.Exit(1)
	// 	}
	// }

	// applyMsg := ApplyMsg{SnapshotValid: true, Snapshot: rf.snapshot, SnapshotTerm: args.LastIncludedTerm, SnapshotIndex: args.LastIncludedIndex}

	// if rf.commitIndex < args.LastIncludedIndex {
	// 	rf.commitIndex = args.LastIncludedIndex
	// }
	// if rf.lastApplied < args.LastIncludedIndex {
	// 	rf.lastApplied = args.LastIncludedIndex
	// }

	// rf.persist()
	// DPrintf("%v: installsnapshot rpc done, discard log, log==%v\n", rf.me, rf.log)
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
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
	DPrintf2("%v: get mutex at start\n", rf.me)
	defer rf.mu.Unlock()
	if rf.state == LEADER {
		index = rf.lastLogIndex() + 1
		term = rf.currentTerm
		isLeader = true
		log := ApplyMsg{Command: command, CommandIndex: index, CommandTerm: term}
		rf.log = append(rf.log, log)
		//DPrintf3("%v raft: log appending, len==%v\n", rf.me, len(rf.log))
		rf.persist()
		rf.nextIndex[rf.me] = index + 1
		rf.matchIndex[rf.me] = index
		DPrintf("%v: receive command from client %v\n", rf.me, rf.log[len(rf.log)-1])
		DPrintf3("%v raft: command %v", rf.me, index)
	}

	DPrintf2("%v: release mutex at start\n", rf.me)
	return index, term, isLeader
}

func (rf *Raft) lastLogIndex() int {
	return rf.log[len(rf.log)-1].CommandIndex
}

func (rf *Raft) lastLogTerm() int {
	return rf.log[len(rf.log)-1].CommandTerm
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
	DPrintf("%v: reset ticker, electionTimeout %v\n", rf.me, rf.electionTimeout)
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
		DPrintf2("%v: get mutex at heartbeat\n", rf.me)
		duration := time.Since(rf.lastHeartbeatTime)
		if duration.Milliseconds() < int64(rf.heartbeatTimeout) {
			rf.mu.Unlock()
			DPrintf2("%v: release mutex at heartbeat\n", rf.me)
			continue
		}
		if rf.state != LEADER {
			rf.mu.Unlock()
			DPrintf2("%v: release mutex at heartbeat\n", rf.me)
			continue
		}

		DPrintf("%v: will send heartbeat to all, term %v\n", rf.me, rf.currentTerm)
		rf.sendHeartbeat()

		rf.lastHeartbeatTime = time.Now()
		rf.mu.Unlock()
		DPrintf2("%v: release mutex at heartbeat\n", rf.me)
	}
}

// Must be call with rf.mu held
func (rf *Raft) sendHeartbeat() {
	//term := rf.currentTerm
	args_tmp := AppendEntriesArgs{Term: rf.currentTerm, LeaderId: rf.me, PrevLogIndex: rf.lastLogIndex(), PrevLogTerm: rf.lastLogTerm(), LeaderCommit: rf.commitIndex}
	reply_tmp := AppendEntriesReply{}

	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(i int, args_tmp AppendEntriesArgs, reply_tmp AppendEntriesReply) {
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
			if reply.Term > args.Term {
				return
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()
			if rf.state != LEADER || rf.currentTerm != args.Term {
				return
			}
			if !ok {
				DPrintf("%v: send heartbeat to %v not OK\n", rf.me, i)
				return
			}
			if reply.Term > rf.currentTerm {
				DPrintf("%v: send heartbeat fail, become follower term %v\n", rf.me, reply.Term)
				rf.becomeFollower(reply.Term)
				return
			}
			if !reply.Success {
				newNext := 0
				for i, log := range rf.log {
					if log.CommandTerm == reply.ConflictTerm {
						newNext = i + 1
						for newNext < len(rf.log) {
							if rf.log[newNext].CommandTerm > reply.ConflictTerm {
								break
							}
							newNext++
						}

						break
					}
				}

				if newNext > 0 {
					rf.nextIndex[i] = newNext
				} else {
					rf.nextIndex[i] = reply.ConflictIndex
				}

				if rf.nextIndex[i] == 0 {
					rf.nextIndex[i] = 1
				}

				DPrintf("%v: send AppendEntries to %v not success, nextIndex[%v] decrease to %v\n", rf.me, i, i, rf.nextIndex[i])
			}
		}(i, args_tmp, reply_tmp)
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

		rf.mu.Lock()
		DPrintf2("%v: get mutex at newElection\n", rf.me)
		DPrintf("%v: starting new election, term %v\n", rf.me, rf.currentTerm+1)
		//DPrintf("candidate wakeup %v get rf.mu\n", rf.me)
		rf.state = CANDIDATE
		rf.currentTerm++
		rf.voteCount = 1
		rf.votedFor = rf.me
		rf.persist()
		rf.resetTicker()

		term := rf.currentTerm
		args_tmp := RequestVoteArgs{Term: rf.currentTerm, CandidateId: rf.me, LastLogIndex: rf.lastLogIndex(), LastLogTerm: rf.lastLogTerm()}

		for i := range rf.peers {
			if i == rf.me {
				continue
			}
			go func(i int, args_tmp RequestVoteArgs) {
				// DPrintf("%v: send requestvote to %v, term %v\n", rf.me, i, rf.currentTerm)
				args := &RequestVoteArgs{}
				*args = args_tmp
				reply := &RequestVoteReply{}
				ok := rf.sendRequestVote(i, args, reply)

				if reply.Term > args.Term {
					return
				}

				rf.mu.Lock()
				DPrintf2("%v: get mutex at newElection sub-goroutine %v\n", rf.me, i)
				defer rf.mu.Unlock()
				if rf.currentTerm != term || rf.state != CANDIDATE || !ok {
					DPrintf("%v: requestvote return but state change or not ok\n", rf.me)
					DPrintf2("%v: release mutex at newElection sub-goroutine %v\n", rf.me, i)
					return
				}
				if reply.Term > rf.currentTerm {
					DPrintf("%v: requestvote return but term too small, become follower, term %v\n", rf.me, reply.Term)
					rf.becomeFollower(reply.Term)
					DPrintf2("%v: release mutex at newElection sub-goroutine %v\n", rf.me, i)
					return
				}
				if reply.VoteGranted {
					DPrintf("%v: receive one vote from %v, term %v\n", rf.me, i, reply.Term)
					DPrintf3("%v raft: receive one vote from %v, term %v\n", rf.me, i, reply.Term)
					rf.voteCount++
				}
				if rf.voteCount > 1 && rf.voteCount > len(rf.peers)/2 {
					DPrintf("%v: receive majority vote, become leader, term %v\n", rf.me, rf.currentTerm)
					DPrintf3("%v raft: receive majority vote, become leader, term %v\n", rf.me, rf.currentTerm)
					rf.becomeLeader()
				}
				DPrintf2("%v: release mutex at newElection sub-goroutine %v\n", rf.me, i)
			}(i, args_tmp)
		}

		rf.mu.Unlock()
		DPrintf2("%v: release mutex at newElection\n", rf.me)
	}
}

// Must be called with rf.mu held
func (rf *Raft) becomeLeader() {
	DPrintf("%v: becoming leader and will sendheatbeat, term==%v, log==%v\n", rf.me, rf.currentTerm, rf.log)
	rf.state = LEADER
	rf.lastHeartbeatTime = time.Now()
	for i := range rf.nextIndex {
		rf.nextIndex[i] = rf.lastLogIndex() + 1
	}
	for i := range rf.matchIndex {
		if i == rf.me {
			rf.matchIndex[i] = rf.lastLogIndex()
		} else {
			rf.matchIndex[i] = 0
		}
	}
	rf.sendHeartbeat()

	// op := Op{}
	// op.EncodeOp(OpFields{opIndex: -1, oper: "None", key: "", ckId: -1})
	// rf.StartNoLock(nil)
}

// Must be called with rf.mu held
func (rf *Raft) becomeFollower(term int) {
	DPrintf("%v: becoming follower, term==%v, log==%v\n", rf.me, rf.currentTerm, rf.log)
	rf.state = FOLLOWER
	rf.currentTerm = term
	rf.votedFor = -1
	rf.persist()
	// rf.resetTicker()
}

func (rf *Raft) commitLogs() {
	for rf.killed() == false {
		time.Sleep(10 * TIME_STEP * time.Millisecond)

		rf.mu.Lock()
		DPrintf2("%v: get mutex at commitLogs\n", rf.me)
		if rf.state != LEADER {
			rf.mu.Unlock()
			DPrintf2("%v: release mutex at commitLogs\n", rf.me)
			continue
		}

		var wg sync.WaitGroup
		wg.Add(len(rf.peers))
		for i := range rf.peers {
			if i == rf.me {
				wg.Done()
				continue
			}

			if rf.nextIndex[i] > rf.lastLogIndex() {
				wg.Done()
				continue
			}
			if rf.nextIndex[i] <= rf.log[0].CommandIndex {
				wg.Done()
				continue
			}
			pos := rf.getPosByIndex(rf.nextIndex[i])
			if pos == -1 {
				fmt.Printf("%v: try to send nextIndex[%v]==%v, but not in log[]\n", rf.me, i, rf.nextIndex[i])
				os.Exit(1)
			}

			args_tmp := AppendEntriesArgs{Term: rf.currentTerm, LeaderId: rf.me, PrevLogIndex: rf.log[pos-1].CommandIndex, PrevLogTerm: rf.log[pos-1].CommandTerm, Entries: rf.log[pos:], LeaderCommit: rf.commitIndex}

			go func(i int, args_tmp AppendEntriesArgs) {
				defer wg.Done()

				args := &AppendEntriesArgs{}
				*args = args_tmp
				reply := &AppendEntriesReply{}
				// DPrintf("%v: send AppendEntries to %v, entries %v, log %v\n", rf.me, i, args.Entries, rf.log)
				ok := rf.sendAppendEntries(i, args, reply)

				if reply.Term > args.Term {
					return
				}

				rf.mu.Lock()
				DPrintf2("%v: get mutex at commitLogs sub-goroutine %v\n", rf.me, i)

				if rf.state != LEADER || rf.currentTerm != args.Term {
					rf.mu.Unlock()
					DPrintf2("%v: release mutex at commitLogs sub-goroutine %v\n", rf.me, i)
					return
				}
				if !ok {
					DPrintf("%v: send AppendEntries to %v not OK\n", rf.me, i)
					rf.mu.Unlock()
					DPrintf2("%v: release mutex at commitLogs sub-goroutine %v\n", rf.me, i)
					return
				}
				if reply.Term > rf.currentTerm {
					DPrintf("%v: send AppendEntries fail, term too small, become follower term %v\n", rf.me, reply.Term)
					rf.becomeFollower(reply.Term)
					rf.mu.Unlock()
					DPrintf2("%v: release mutex at commitLogs sub-goroutine %v\n", rf.me, i)
					return
				}
				if reply.Success {
					rf.nextIndex[i] = args.PrevLogIndex + len(args.Entries) + 1
					// DPrintf("%v: %v %v %v\n", rf.me, rf.nextIndex[i], args.PrevLogIndex, len(args.Entries))
					rf.matchIndex[i] = rf.nextIndex[i] - 1
					DPrintf("%v: send AppendEntries to %v success, nextIndex[%v] updated to %v\n", rf.me, i, i, rf.nextIndex[i])
					rf.mu.Unlock()
					DPrintf2("%v: release mutex at commitLogs sub-goroutine %v\n", rf.me, i)
					return
				} else {
					newNext := 0
					for i, log := range rf.log {
						if log.CommandTerm == reply.ConflictTerm {
							newNext = i + 1
							for newNext < len(rf.log) {
								if rf.log[newNext].CommandTerm > reply.ConflictTerm {
									break
								}
								newNext++
							}

							break
						}
					}

					if newNext > 0 {
						rf.nextIndex[i] = newNext
					} else {
						rf.nextIndex[i] = reply.ConflictIndex
					}

					if rf.nextIndex[i] == 0 {
						rf.nextIndex[i] = 1
					}

					DPrintf("%v: send AppendEntries to %v not success, nextIndex[%v] decrease to %v\n", rf.me, i, i, rf.nextIndex[i])
				}

				rf.mu.Unlock()
				DPrintf2("%v: release mutex at commitLogs sub-goroutine %v\n", rf.me, i)
			}(i, args_tmp)
		}

		rf.mu.Unlock()
		DPrintf2("%v: release mutex at commitLogs\n", rf.me)

		// wg.Wait()
	}
}

func (rf *Raft) applyLogs() {
	for rf.killed() == false {
		// time.Sleep(2 * TIME_STEP * time.Millisecond)
		time.Sleep(TIME_STEP * time.Millisecond)

		rf.mu.Lock()
		DPrintf2("%v: get mutex at applyLogs\n", rf.me)
		for rf.commitIndex > rf.lastApplied {
			rf.lastApplied++

			if rf.lastApplied <= rf.log[0].CommandIndex {
				DPrintf("%v: applying log fail due to rf.lastApplied <= rf.log[0].CommandIndex, log:%v, commitIndex:%v, lastApplied:%v\n", rf.me, rf.log, rf.commitIndex, rf.lastApplied)
				rf.lastApplied--
				goto applyLogsLoopExit
			}

			DPrintf1("%v: test apply, lastApplied==%v commitIndex==%v rf.log==%v, rf.snapshot==%v\n", rf.me, rf.lastApplied, rf.commitIndex, rf.log, rf.snapshot)
			// if rf.lastApplied <= rf.log[0].CommandIndex {
			// 	applyMsg := ApplyMsg{SnapshotValid: true, Snapshot: rf.snapshot, SnapshotTerm: rf.log[0].CommandTerm, SnapshotIndex: rf.log[0].CommandIndex}
			// 	select {
			// 	case rf.applyCh <- applyMsg:
			// 		rf.lastApplied = rf.log[0].CommandIndex
			// 		DPrintf("%v: applying log success, lastApplied==%v, with snapshot==%v\n", rf.me, rf.lastApplied, rf.snapshot)
			// 		//DPrintf3("%v raft: applying log success, lastApplied==%v, with snapshot==%v\n", rf.me, rf.lastApplied, rf.snapshot)
			// 	default:
			// 		DPrintf("%v: applying log fail due to block, lastApplied==%v\n", rf.me, rf.lastApplied)
			// 		//DPrintf3("%v raft: applying log fail due to block, lastApplied==%v\n", rf.me, rf.lastApplied)
			// 		rf.lastApplied--
			// 		goto applyLogsLoopExit
			// 	}
			// }

			pos := rf.getPosByIndex(rf.lastApplied)
			if pos == -1 {
				fmt.Printf("%v: applying log fail due to pos not exists, log:%v, commitIndex:%v, lastApplied:%v\n", rf.me, rf.log, rf.commitIndex, rf.lastApplied)
				os.Exit(1)
			}

			DPrintf("%v: applying log, lastApplied==%v pos==%v commitIndex==%v rf.log==%v\n", rf.me, rf.lastApplied, pos, rf.commitIndex, rf.log)
			//DPrintf3("%v raft: applying log, lastApplied==%v pos==%v commitIndex==%v rf.log==%v\n", rf.me, rf.lastApplied, pos, rf.commitIndex, rf.log)
			log := rf.log[pos]

			log.CommandValid = true
			select {
			case rf.applyCh <- log:
				DPrintf("%v: applying log success, lastApplied==%v\n", rf.me, rf.lastApplied)
				//DPrintf3("%v raft: applying log success, lastApplied==%v\n", rf.me, rf.lastApplied)
			default:
				DPrintf("%v: applying log fail due to block, lastApplied==%v\n", rf.me, rf.lastApplied)
				//DPrintf3("%v raft: applying log fail due to block, lastApplied==%v\n", rf.me, rf.lastApplied)
				rf.lastApplied--
				goto applyLogsLoopExit
			}
		}
	applyLogsLoopExit:
		rf.mu.Unlock()
		DPrintf2("%v: release mutex at applyLogs\n", rf.me)
	}
}

func (rf *Raft) updateCommitIndex() {
	for rf.killed() == false {
		// time.Sleep(2 * TIME_STEP * time.Millisecond)
		time.Sleep(TIME_STEP * time.Millisecond)

		rf.mu.Lock()
		DPrintf2("%v: get mutex at updateCommitIndex\n", rf.me)

		if rf.state != LEADER {
			rf.mu.Unlock()
			DPrintf2("%v: release mutex at updateCommitIndex\n", rf.me)
			continue
		}

		// if rf.lastIncludedTerm == rf.currentTerm {
		// 	N := rf.lastIncludedIndex
		// 	if N > rf.commitIndex {
		// 		count := 0
		// 		for j := range rf.matchIndex {
		// 			if rf.matchIndex[j] >= N {
		// 				count++
		// 			}
		// 		}

		// 		if count > len(rf.peers)/2 {
		// 			rf.commitIndex = N
		// 			DPrintf("%v: commitIndex updated to %v\n", rf.me, rf.commitIndex)
		// 		}
		// 	}
		// }

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
					// DPrintf("%v\n", j)
					count++
				}
			}

			if count > len(rf.peers)/2 {
				rf.commitIndex = N
				DPrintf("%v: commitIndex updated to %v\n", rf.me, rf.commitIndex)
			}
		}

		rf.mu.Unlock()
		DPrintf2("%v: release mutex at updateCommitIndex\n", rf.me)
	}
}

func (rf *Raft) sendSnapshots() {
	for rf.killed() == false {
		time.Sleep(10 * TIME_STEP * time.Millisecond)
		// time.Sleep(TIME_STEP * time.Millisecond)

		rf.mu.Lock()
		DPrintf2("%v: get mutex at sendSnapshots\n", rf.me)

		if rf.state != LEADER {
			rf.mu.Unlock()
			DPrintf2("%v: release mutex at sendSnapshots due to not leader\n", rf.me)
			continue
		}

		var wg sync.WaitGroup
		wg.Add(len(rf.peers))
		for i := range rf.peers {
			if i == rf.me {
				wg.Done()
				continue
			}

			if rf.nextIndex[i] > rf.log[0].CommandIndex {
				wg.Done()
				continue
			}

			snapshot := rf.snapshot
			term := rf.currentTerm
			me := rf.me
			lastIdx := rf.log[0].CommandIndex
			lastTerm := rf.log[0].CommandTerm

			go func(i int) {
				defer wg.Done()

				args := &InstallSnapshotArgs{Term: term, LeaderId: me, LastIncludedIndex: lastIdx, LastIncludedTerm: lastTerm, Offset: 0, Data: snapshot, Done: true}
				reply := &InstallSnapshotReply{}
				DPrintf("%v: send snapshot to %v, args==%v, lastIncludedIndex==%v\n", me, i, args, lastIdx)
				ok := rf.sendInstallSnapshot(i, args, reply)
				// wait for reply, but other goroutines can run now
				if !ok {
					DPrintf2("%v: release mutex at sendSnapshotToPeer %v, ending\n", rf.me, i)
					return
				}
				if reply.Term > args.Term {
					return
				}

				rf.mu.Lock()
				DPrintf2("%v: get mutex at sendSnapshotToPeer %v on reply\n", rf.me, i)

				if rf.state != LEADER || rf.currentTerm != args.Term {
					rf.mu.Unlock()
					DPrintf2("%v: release mutex at sendSnapshotToPeer %v, ending\n", rf.me, i)
					return
				}
				// if rf.nextIndex[i] > rf.log[0].CommandIndex {
				// 	break
				// }
				if rf.nextIndex[i] > lastIdx {
					rf.mu.Unlock()
					DPrintf2("%v: release mutex at sendSnapshotToPeer %v, ending\n", rf.me, i)
					return
				}

				rf.nextIndex[i] = lastIdx + 1
				rf.mu.Unlock()
			}(i)
		}

		rf.mu.Unlock()
		DPrintf2("%v: release mutex at sendSnapshots\n", rf.me)

		// wg.Wait()
	}
}

// func (rf *Raft) sendSnapshotToPeer(i int) {
// 	if rf.killed() == false {
// 		rf.mu.Lock()
// 		DPrintf2("%v: get mutex at sendSnapshotToPeer %v\n", rf.me, i)

// 		if rf.state != LEADER {
// 			rf.mu.Unlock()
// 			DPrintf2("%v: release mutex at sendSnapshotToPeer %v due to not leader\n", rf.me, i)
// 			return
// 		}

// 		if rf.nextIndex[i] > rf.log[0].CommandIndex {
// 			rf.mu.Unlock()
// 			DPrintf2("%v: release mutex at sendSnapshotToPeer %v due to index, rf.nextIndex[%v]==%v, rf.log[0].CommandIndex==%v\n", rf.me, i, i, rf.nextIndex[i], rf.log[0].CommandIndex)
// 			return
// 		}

// 		snapshot := rf.snapshot
// 		pos := 0
// 		term := rf.currentTerm
// 		lastIdx := rf.log[0].CommandIndex
// 		lastTerm := rf.log[0].CommandTerm
// 		for pos < len(snapshot) {
// 			data := snapshot[pos:min(pos+CHUNK_SIZE, len(snapshot))]
// 			done := (pos+CHUNK_SIZE >= len(snapshot))
// 			args := &InstallSnapshotArgs{Term: term, LeaderId: rf.me, LastIncludedIndex: lastIdx, LastIncludedTerm: lastTerm, Offset: pos, Data: data, Done: done}
// 			reply := &InstallSnapshotReply{}
// 			rf.mu.Unlock()
// 			DPrintf2("%v: release mutex at sendSnapshotToPeer %v, will send\n", rf.me, i)
// 			DPrintf("%v: send snapshot to %v, args==%v, lastIncludedIndex==%v\n", rf.me, i, args, rf.log[0].CommandIndex)
// 			ok := rf.sendInstallSnapshot(i, args, reply)
// 			// wait for reply, but other goroutines can run now
// 			rf.mu.Lock()
// 			DPrintf2("%v: get mutex at sendSnapshotToPeer %v on reply\n", rf.me, i)
// 			if !ok {
// 				break
// 			}
// 			if rf.state != LEADER {
// 				break
// 			}
// 			if rf.nextIndex[i] > rf.log[0].CommandIndex {
// 				break
// 			}

// 			pos += CHUNK_SIZE
// 		}

// 		rf.nextIndex[i] = rf.log[0].CommandIndex + 1
// 		rf.mu.Unlock()
// 		DPrintf2("%v: release mutex at sendSnapshotToPeer %v, ending\n", rf.me, i)
// 	}
// }

// Must be called with rf.mu held
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
	// labgob.Register(Op{})

	rf.mu.Lock()
	DPrintf2("%v: get mutex at make\n", rf.me)
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
	// rf.timeoutBase = 300
	// rf.timeoutOffset = 150
	rf.timeoutBase = 150
	rf.timeoutOffset = 150
	rf.electionTimeout = rf.timeoutBase + rand.Intn(rf.timeoutOffset)
	rf.lastHeartbeatTime = time.Now()
	rf.heartbeatTimeout = 50
	rf.applyCh = applyCh

	rf.snapshot = make([]byte, 0)
	rf.snapshotBuffer = make([]byte, 0)

	rf.cond = sync.NewCond(&rf.condLock)
	rf.wakeUpType = NONE

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.snapshot = rf.persister.ReadSnapshot()

	i := 1
	if i < len(rf.log) && rf.log[i].CommandIndex <= rf.log[0].CommandIndex {
		i++
	}
	if i > 1 {
		rf.log = append(rf.log[0:1], rf.log[i:]...)
	}

	DPrintf("%v: make read snapshot %v\n", rf.me, rf.snapshot)

	rf.mu.Unlock()
	DPrintf2("%v: release mutex at make\n", rf.me)

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.heartbeat()
	go rf.newElection()
	go rf.commitLogs()
	go rf.applyLogs()
	go rf.updateCommitIndex()
	go rf.sendSnapshots()

	DPrintf("%v: server start\n", rf.me)
	DPrintf3("%v raft: raft start\n", rf.me)

	return rf
}
