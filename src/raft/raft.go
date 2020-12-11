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
	"fmt"
	"math/rand"
	"sync"
	"time"
)
import "labrpc"

// import "bytes"
// import "labgob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

//
// A Go object implementing a single Raft peer.
//
type State uint8

const (
	FOLLOWER  State = 0
	CANDIDATE State = 1
	LEADER    State = 2
)

type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	// TODO: Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int // latest term server has seen
	votedFor    int // candidateId that received vote in current term
	state       State
	voteCh      chan struct{}
	appendCh    chan struct{}
	timeOut     time.Duration
	timer       *time.Timer
	voteCount   int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.currentTerm
	isleader = rf.state == LEADER
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
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// TODO: Your data here (2A, 2B).
	Term         int // candidate’s term
	CandidateId  int // candidate requesting vote
	LastLogIndex int // index of candidate’s last log entry
	LastLogTerm  int // term of candidate’s last log entry
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// TODO: Your data here (2A).
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

// TODO: 完成entries、AppendEntriesArgs和AppendEntriesReply结构体的定义
type entry struct {
	Commend string
	Term    int
	Index   int
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PreLogTerm   int
	Entries      []entry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// TODO: Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term > rf.currentTerm && rf.votedFor == -1 {
		rf.currentTerm = args.Term
		rf.votedFor = args.CandidateId
		rf.state = FOLLOWER
		reply.VoteGranted = true
	} else if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
	}

	go func() {
		rf.voteCh <- struct{}{}
	}()
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

// TODO: 完成AppendEntries函数体
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
	} else if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = FOLLOWER
		rf.votedFor = -1
		reply.Success = true
	} else {
		reply.Success = true
	}
	go func() {
		rf.appendCh <- struct{}{}
	}()
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
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

func (rf *Raft) tranState(state State) {
	if rf.state == state {
		return
	}
	if state == FOLLOWER {
		rf.state = FOLLOWER
		fmt.Printf("raft%v become follower\n", rf.me)
		rf.votedFor = -1
	}
	if state == CANDIDATE {
		rf.state = CANDIDATE
		fmt.Printf("raft%v become candidate\n", rf.me)
		rf.election()
	}
	if state == LEADER {
		rf.state = LEADER
		fmt.Printf("raft%v become leader\n", rf.me)
	}
}

func (rf *Raft) election() {
	rf.currentTerm += 1
	rf.votedFor = rf.me
	rf.timer.Reset(rf.timeOut)
	rf.voteCount = 1
	argv := RequestVoteArgs{}
	argv.Term = rf.currentTerm
	argv.CandidateId = rf.me
	replyv := RequestVoteReply{}
	for i, _ := range rf.peers {
		if i != rf.me {
			go func(server int) {
				if rf.state == CANDIDATE && rf.sendRequestVote(server, &argv, &replyv) {
					if replyv.VoteGranted {
						rf.voteCount += 1
					} else if replyv.Term > rf.currentTerm {
						rf.mu.Lock()
						rf.currentTerm = replyv.Term
						rf.tranState(FOLLOWER)
						rf.mu.Unlock()
					}
				} else {
					//	fmt.Printf("raft%v no reply, currentStat:%v\n", server, rf.stat)
				}
			}(i)
		}
	}
}

func (rf *Raft) startLoop() {
	for {
		switch rf.state {
		case FOLLOWER:
			select {
			case <-rf.voteCh:
				rf.timer.Reset(rf.timeOut)
			case <-rf.appendCh:
				rf.timer.Reset(rf.timeOut)
			case <-rf.timer.C:
				rf.tranState(CANDIDATE)
			}
		case CANDIDATE:
			select {
			case <-rf.appendCh:
				rf.timer.Reset(rf.timeOut)
				rf.mu.Lock()
				rf.tranState(FOLLOWER)
				rf.mu.Unlock()
			case <-rf.timer.C:
				rf.election()
			default:
				if rf.voteCount > len(rf.peers)/2 {
					rf.mu.Lock()
					rf.tranState(LEADER)
					rf.mu.Unlock()
				}
			}
		case LEADER:
			for i, _ := range rf.peers {
				if i != rf.me {
					go func(server int) {
						argv := AppendEntriesArgs{}
						argv.Term = rf.currentTerm
						argv.Entries = []entry{}
						argv.LeaderId = rf.me
						replyv := AppendEntriesReply{}
						rf.sendAppendEntries(server, &argv, &replyv)
						if replyv.Term > rf.currentTerm {
							rf.mu.Lock()
							rf.currentTerm = replyv.Term
							rf.tranState(FOLLOWER)
							rf.mu.Unlock()
						}
					}(i)
				}
			}
			time.Sleep(100 * time.Millisecond)
		}
	}
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
	// TODO: Your initialization code here (2A, 2B, 2C).
	//fmt.Println("rf初始化……")
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.state = FOLLOWER
	rf.voteCh = make(chan struct{})
	rf.appendCh = make(chan struct{})

	ms := time.Duration(150+(rand.Int63()%150)) * time.Millisecond
	rf.timeOut = ms
	rf.timer = time.NewTimer(rf.timeOut)
	go rf.startLoop()
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	return rf
}
