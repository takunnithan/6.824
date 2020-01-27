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
	"labrpc"
	"sync"
	"time"
)

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

const FOLLOWER string = "FOLLOWER"
const CANDIDATE string = "CANDIDATE"
const LEADER string = "LEADER"

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type Log struct {
	Command string
	Term    int
}
type NextIndex struct {
	Index  int
	Server int
}

type MatchIndex struct {
	Index  int
	Server int
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Log
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu               sync.Mutex          // Lock to protect shared access to this peer's state
	peers            []*labrpc.ClientEnd // RPC end points of all peers
	persister        *Persister          // Object to hold this peer's persisted state
	me               int                 // this peer's index into peers[]
	currentTerm      int
	votedFor         int
	log              []Log
	commitIndex      int
	lastAppliedIndex int
	nextIndex        []NextIndex
	matchIndex       []MatchIndex
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	electionTimer int
	state         string
	otherServers  []int
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

func (rf *Raft) acceptVoteRequest(args *RequestVoteArgs, reply *RequestVoteReply) {
	reply.Term = args.Term
	reply.VoteGranted = true

	// Updating rf
	rf.votedFor = args.CandidateId
	rf.currentTerm = args.Term
	rf.state = FOLLOWER
	rf.electionTimer = GetRandomElectionTimeout()
}

func (rf *Raft) rejectVoteRequest(reply *RequestVoteReply) {
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
	}
	if rf.votedFor == args.CandidateId || rf.votedFor == 1111 {
		if len(rf.log) > 0 {	// No logs
			followerLastLog := rf.log[len(rf.log)-1]
			if args.LastLogTerm >= followerLastLog.Term {
				if args.LastLogTerm == followerLastLog.Term && args.LastLogIndex < len(rf.log)-1 {
					rf.rejectVoteRequest(reply)
					return
				}
				rf.acceptVoteRequest(args, reply)
				return
			} else {
				rf.rejectVoteRequest(reply)
			}
		} else {
			rf.acceptVoteRequest(args, reply)
		}
	}
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


func (rf *Raft) getRequestVoteArgs() *RequestVoteArgs {
	LastLogIndex := 1111
	LastLogTerm := 1111
	if len(rf.log) > 0 {
		LastLogIndex = len(rf.log) - 1
		LastLogTerm = rf.log[LastLogIndex].Term
	}
	return &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: LastLogIndex,
		LastLogTerm:  LastLogTerm}
}

func (rf *Raft) HandleElection() {
	majority := GetMajority(len(rf.peers))
	for {
		if rf.state == CANDIDATE {
			rf.currentTerm = rf.currentTerm + 1
			args := rf.getRequestVoteArgs()
			electionTerm := rf.currentTerm
			rf.votedFor = 1111
			voteGranted := 1
			rf.electionTimer = GetRandomElectionTimeout()
			var wg sync.WaitGroup
			wg.Add(len(rf.otherServers))
			for _, server := range rf.otherServers {
				go func(rf *Raft, server int, args *RequestVoteArgs, voteGranted *int, wg *sync.WaitGroup) {
					reply := &RequestVoteReply{}
					response := rf.sendRequestVote(server, args, reply)
					//for rf.electionTimer > 0 {
					for response {
						if reply.VoteGranted {
							*voteGranted = *voteGranted + 1 // Potential data race
							break
						} else {
							if reply.Term > electionTerm {
								rf.currentTerm = reply.Term
								rf.votedFor = 1111
								break
							}
						}
						//wg.Done()
					}
					wg.Done()
				}(rf, server, args, &voteGranted, &wg)
			}
			wg.Wait() // Hopefully RPC Call() will have a timeout (smaller than election tmout)
			if rf.electionTimer > 0 {
				if rf.currentTerm == electionTerm {
					if voteGranted >= majority {
						rf.state = LEADER
						rf.electionTimer = GetRandomElectionTimeout()
						fmt.Println("I AM THE LEADER", rf.me, "majority: ", majority, "Votes: ", voteGranted)
					}
				}
			}
		}
		time.Sleep(1 * time.Microsecond)
	}
}

func (rf *Raft) convertToFollower() {
	rf.state = FOLLOWER
	rf.electionTimer = GetRandomElectionTimeout()
}

func (rf *Raft) convertToCandidate() {
	for {
		// <<<< May be use Channels b/w ElectionTimerCounter() & convertToCandidate()  >>>
		// if rf.electionTimer == 0 && rf.state == FOLLOWER {
		if rf.electionTimer == 0 && rf.state != CANDIDATE{
			rf.state = CANDIDATE
			fmt.Println(rf.me, " has become a candidate")
		}
		time.Sleep(5 * time.Microsecond)
	}
}

func (rf *Raft) ElectionTimerCounter() {
	for {
		if rf.electionTimer > 0  && rf.state != LEADER {
			rf.electionTimer = rf.electionTimer - 1
		}
		time.Sleep(1 * time.Millisecond)
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) getAppendEntriesArgs() *AppendEntriesArgs {
	previousLogIndex := 1111 // Used 1111 Instead of nil
	previousLogTerm := 1111
	if len(rf.log) >= 2 {
		previousLogIndex := len(rf.log) - 2
		previousLog := rf.log[previousLogIndex]
		previousLogTerm = previousLog.Term
	}
	args := &AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: previousLogIndex,
		PrevLogTerm:  previousLogTerm,
		Entries:      nil, // Update this for AppendEntries
		LeaderCommit: rf.commitIndex}
	return args
}

// Does this send heartbeats immediately , when a candidate becomes a leader  ????
func (rf *Raft) HeartBeat() {
	for {
		if rf.state == LEADER {
			args := rf.getAppendEntriesArgs()
			reply := &AppendEntriesReply{}
			for _, server := range rf.otherServers {
				go func(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
					response := rf.sendAppendEntries(server, args, reply)
					for {
						if response {
							if reply.Term > rf.currentTerm {
								rf.currentTerm = reply.Term
								rf.votedFor = 1111
								rf.convertToFollower()
							}
							break
						}
					}
				}(server, args, reply)
			}
			time.Sleep(10 * time.Millisecond)
		}
		time.Sleep(2 * time.Microsecond)
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Check if it is a heartbeat or regular append entries RPC
	// check the conditions
	//fmt.Println("term: ", args.Term, "Leader: ", args.LeaderId, " me: ", rf.me, " my term: ", rf.currentTerm)
	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.votedFor = 1111
	}
	// Do the other checks for AppendEntries
	// Only reset election timer for heart beats
	rf.convertToFollower()
	reply.Success = true
	reply.Term = rf.currentTerm
	return
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

func (rf *Raft) getThePeers() {
	for index := range rf.peers {
		if index != rf.me {
			rf.otherServers = append(rf.otherServers, index)
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
	rf.votedFor = 1111
	rf.currentTerm = 0
	rf.state = FOLLOWER
	rf.electionTimer = GetRandomElectionTimeout()
	fmt.Println(rf.electionTimer)

	// Your initialization code here (2A, 2B, 2C).
	rf.getThePeers()
	go rf.ElectionTimerCounter()
	go rf.convertToCandidate()
	go rf.HandleElection()
	go rf.HeartBeat()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
