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
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

// storing log entries
type LogEntry struct {
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu              sync.Mutex          // Lock to protect shared access to this peer's state
	peers           []*labrpc.ClientEnd // RPC end points of all peers
	persister       *Persister          // Object to hold this peer's persisted state
	me              int                 // this peer's index into peers[]
	LogEntries      []LogEntry          // To store log entries
	ElectionTimeOut int
	Term            int // Current term of the server
	LastLogIndex    int
	LastLogTerm     int
	IsLeader        bool
	VotedFor        map[int]int

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	term := rf.Term
	isLeader := rf.IsLeader
	rf.mu.Unlock()
	return term, isLeader
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
	Term         int // candidate’s term
	CandidateID  int // candidate requesting vote
	LastLogIndex int // index of candidate’s last log entry
	LastLogTerm  int // term of candidate’s last log entry
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  //  currentTerm for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	// TODO: Write the Handler code here.
	// Check the term, who is the current leader, etc -> Refer Raft paper
	// Send the reply struct.
	// For now -> if election timer is not timed out - send NO! & current Term
	_, voted := rf.VotedFor[args.Term]
	if (args.Term >= rf.Term) && (!voted) {
		rf.mu.Lock()
		rf.IsLeader = false
		rf.ElectionTimeOut = GetRandomElectionTimeout()
		reply.Term = rf.Term
		reply.VoteGranted = true
		rf.Term = args.Term
		rf.VotedFor[rf.Term] = args.CandidateID
		rf.mu.Unlock()
		fmt.Println(rf.me, " Voted for: ", args.CandidateID, "Term:", args.Term)
	} else {
		reply.Term = rf.Term
		reply.VoteGranted = false
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
	// Don't send the rpc to yourself
	// fmt.Println(server, len(rf.peers))
	if rf.peers[server] != rf.peers[rf.me] {
		// ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
		// return ok
		rf.peers[server].Call("Raft.RequestVote", args, reply)
	}
	return true
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
	rf.Term = 0
	rf.IsLeader = false
	rf.ElectionTimeOut = GetRandomElectionTimeout()
	rf.VotedFor = make(map[int]int)
	fmt.Println(rf.ElectionTimeOut)
	// Your initialization code here (2A, 2B, 2C).

	//   All Goroutines must be run in background indefinitely
	go rf.ElectionTimerCounter()

	// TODO: A goroutine to check the heartbeat messages from leader
	// If the timer times out -> send sendRequestVote call to other raft servers.
	// Need to decide the election timeout
	go rf.ElectLeader()

	go rf.sendHeartBeat()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}

type AppendEntries struct {
	HeartBeat bool
	Term      int
	Sender    int
}

type HeartBeatReply struct {
	IamAlive bool
}

func (rf *Raft) ImmediateHeartBeat() {
	heartBeat := &AppendEntries{HeartBeat: true, Sender: rf.me, Term: rf.Term}
	heartBeatReply := &HeartBeatReply{}
	for i := 0; i <= (len(rf.peers) - 1); i++ {
		if rf.peers[i] != rf.peers[rf.me] {
			rf.peers[i].Call("Raft.HandleHeartBeat", heartBeat, heartBeatReply)
		}
	}
}

func (rf *Raft) sendHeartBeat() {
	heartBeat := &AppendEntries{}
	heartBeat.HeartBeat = true
	heartBeat.Sender = rf.me
	heartBeatReply := &HeartBeatReply{}
	for {
		rf.mu.Lock()
		heartBeat.Term = rf.Term
		isLeader := rf.IsLeader
		rf.mu.Unlock()
		if isLeader {
			for i := 0; i <= (len(rf.peers) - 1); i++ {
				if rf.peers[i] != rf.peers[rf.me] {
					rf.peers[i].Call("Raft.HandleHeartBeat", heartBeat, heartBeatReply)
				}
			}

		}
		time.Sleep(110 * time.Millisecond)
	}
}

func (rf *Raft) HandleHeartBeat(heartBeat *AppendEntries, heartBeatReply *HeartBeatReply) {
	// If a Leader receives a heartbeat --> That means its not a leader anymore
	rf.mu.Lock()
	isNewHeartBeat := rf.Term <= heartBeat.Term
	rf.mu.Unlock()
	if isNewHeartBeat {
		rf.mu.Lock()
		rf.IsLeader = false
		rf.ElectionTimeOut = GetRandomElectionTimeout()
		fmt.Println("Heart Beat:", "receiver:", rf.me, "sender:", heartBeat.Sender, "rec_term", rf.Term, "send_term", heartBeat.Term)
		rf.Term = heartBeat.Term
		rf.mu.Unlock()
		// heartBeatReply.IamAlive = true
	}
}

// Run this function as go routine indefinetly
func (rf *Raft) ElectionTimerCounter() {
	for {
		// Don't timeout if it is the leader
		rf.mu.Lock()
		if !rf.IsLeader {
			rf.ElectionTimeOut = rf.ElectionTimeOut - 1
		}
		rf.mu.Unlock()
		time.Sleep(1 * time.Millisecond)
	}
}

func (rf *Raft) ElectLeader() {
	for {
		rf.mu.Lock()
		timeout := rf.ElectionTimeOut
		isLeader := rf.IsLeader
		rf.mu.Unlock()
		if timeout <= 0 && !isLeader {
			rf.mu.Lock()
			rf.Term = rf.Term + 1 // Incrementing term
			votes := 1            // Self voting
			requestArgs := &RequestVoteArgs{
				Term:         rf.Term,
				CandidateID:  rf.me,
				LastLogIndex: rf.LastLogIndex,
				LastLogTerm:  rf.LastLogTerm,
			}
			rf.mu.Unlock()
			majority := GetMajority(len(rf.peers))
			fmt.Println("majority:", majority, "node:", rf.me, "No of peers", len(rf.peers))
			for i := 0; i <= (len(rf.peers) - 1); i++ {
				go func(vote *int, server int) {
					reply := &RequestVoteReply{}
					rf.sendRequestVote(server, requestArgs, reply)
					// time.Sleep(3 * time.Millisecond)
					if reply.VoteGranted {
						rf.mu.Lock()
						*vote = *vote + 1
						rf.mu.Unlock()
					}
				}(&votes, i)
			}
			time.Sleep(2 * time.Millisecond)
			rf.mu.Lock()
			electionWon := votes >= majority
			rf.mu.Unlock()
			if electionWon {
				rf.mu.Lock()
				rf.IsLeader = true
				rf.mu.Unlock()
				rf.ImmediateHeartBeat()
				fmt.Println(rf.me, " is the Leader...")

			} else {
				rf.mu.Lock()
				rf.IsLeader = false
				rf.mu.Unlock()
			}
			rf.mu.Lock()
			rf.ElectionTimeOut = GetRandomElectionTimeout()
			rf.mu.Unlock()

		}
	}

}
