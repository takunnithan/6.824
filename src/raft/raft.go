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
	fmt.Println("Recieved Vote Reques - me:", rf.me, "from:", args.CandidateID)
	rf.mu.Lock()
	_, voted := rf.VotedFor[args.Term]
	currentTerm := rf.Term
	rf.mu.Unlock()

	// Only accept the request votes from candidates who has higher term -- Not even equal
	// If you are running an election and get a vote request with same term -- means the other server
	// is a your opponent -- No need to vote since you already voted for yourself.

	// WRONG -- args.Term > currentTerm

	if (args.Term >= currentTerm) && (!voted) {

		// Stop any ongoing election for older terms

		rf.mu.Lock()
		rf.IsLeader = false
		rf.ElectionTimeOut = GetRandomElectionTimeout()
		rf.Term = args.Term
		rf.VotedFor[rf.Term] = args.CandidateID
		rf.mu.Unlock()
		reply.Term = rf.Term
		reply.VoteGranted = true
		fmt.Println(rf.me, " Voted for: ", args.CandidateID, "Term:", args.Term)
	} else {
		rf.mu.Lock()
		reply.Term = rf.Term
		rf.mu.Unlock()
		reply.VoteGranted = false
		fmt.Println("Vote rejected: me:", rf.me, "M-Term:", rf.Term, "Candidate:", args.CandidateID, "C-Term:", args.Term)
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
		fmt.Println("Requesting vote: FROM:", rf.me, "TO:", server)
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

type AppendEntriesReply struct {
	Term int
}

func (rf *Raft) ImmediateHeartBeat() {
	heartBeat := &AppendEntries{HeartBeat: true, Sender: rf.me, Term: rf.Term}
	for i := 0; i <= (len(rf.peers) - 1); i++ {
		if rf.peers[i] != rf.peers[rf.me] {
			go func(rf *Raft, server int, heartBeat *AppendEntries) {
				heartBeatReply := &AppendEntriesReply{}
				rf.peers[server].Call("Raft.AppendEntries", heartBeat, heartBeatReply)
				time.Sleep(2 * time.Millisecond)
				if heartBeatReply.Term > rf.Term {
					rf.Term = heartBeatReply.Term
					rf.IsLeader = false
				}
			}(rf, i, heartBeat)
		}
	}
}

func (rf *Raft) sendHeartBeat() {
	for {
		rf.mu.Lock()
		heartBeat := &AppendEntries{HeartBeat: true, Sender: rf.me, Term: rf.Term}
		isLeader := rf.IsLeader
		rf.mu.Unlock()
		if isLeader {
			for i := 0; i <= (len(rf.peers) - 1); i++ {
				if rf.peers[i] != rf.peers[rf.me] {
					go func(rf *Raft, server int, heartBeat *AppendEntries) {
						heartBeatReply := &AppendEntriesReply{}
						rf.peers[server].Call("Raft.AppendEntries", heartBeat, heartBeatReply)
						time.Sleep(2 * time.Millisecond)
						if heartBeatReply.Term > rf.Term {
							rf.Term = heartBeatReply.Term
							rf.IsLeader = false
						}
					}(rf, i, heartBeat)
				}
			}

		}
		time.Sleep(110 * time.Millisecond)
	}
}

func (rf *Raft) AppendEntries(appendEntries *AppendEntries, appendEntriesReply *AppendEntriesReply) {
	// Handling HeartBeats
	if appendEntries.HeartBeat {
		// If a Leader receives a heartbeat --> That means its not a leader anymore
		rf.mu.Lock()
		isNewTerm := rf.Term <= appendEntries.Term
		rf.mu.Unlock()
		if isNewTerm {

			// If there is an ongoing election stop it -- Stop Election

			rf.mu.Lock()
			rf.IsLeader = false
			rf.ElectionTimeOut = GetRandomElectionTimeout()
			rf.Term = appendEntries.Term
			rf.mu.Unlock()
			fmt.Println("Heart Beat:", "receiver:", rf.me, "sender:", appendEntries.Sender, "rec_term", rf.Term, "send_term", appendEntries.Term)
			appendEntriesReply.Term = appendEntries.Term
		} else {
			rf.mu.Lock()
			appendEntriesReply.Term = rf.Term
			rf.mu.Unlock()
		}
	}
}

// Run this function as go routine indefinetly
func (rf *Raft) ElectionTimerCounter() {
	for {
		// Don't timeout if it is the leader
		// If the timer is already 0 then don't hold the locks
		rf.mu.Lock()
		if !rf.IsLeader && rf.ElectionTimeOut > 0 {
			rf.ElectionTimeOut = rf.ElectionTimeOut - 1
		}
		rf.mu.Unlock()
		time.Sleep(1 * time.Millisecond)
	}
}

func (rf *Raft) ElectLeader() {
	for {

		// We cannot lock a variable in all the places in a program
		// This lock will only lock this particular portion of the pgrm.
		// Other places where they access these variables are not locked.

		rf.mu.Lock()
		timeout := rf.ElectionTimeOut
		isLeader := rf.IsLeader
		rf.mu.Unlock()
		if timeout <= 0 && !isLeader {

			// Reset the election timer here when you start a new election!!

			rf.mu.Lock()
			rf.Term = rf.Term + 1 // Incrementing term

			// This self voting should be added to the `VotedFor` of the new term

			votes := 1 // Self voting
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
				if rf.getElectionTimeout() > 0 {
					break
				}
				go func(vote *int, server int) {
					reply := &RequestVoteReply{}
					if rf.getElectionTimeout() > 0 {
						return
					}
					rf.sendRequestVote(server, requestArgs, reply)
					// time.Sleep(2 * time.Millisecond)

					// Wait until the reply has value -- In real RPC we will have to wait for a reply RPC

					if reply.VoteGranted {
						rf.mu.Lock()
						*vote = *vote + 1
						rf.mu.Unlock()
					} else {
						rf.mu.Lock()
						currentTerm := rf.Term
						rf.mu.Unlock()
						if reply.Term > currentTerm {
							rf.mu.Lock()

							// The current election is still in progress -> But Term is updated -- WRONG --
							// Find a way to stop election --> channels ??

							rf.Term = reply.Term
							rf.ElectionTimeOut = GetRandomElectionTimeout()
							rf.mu.Unlock()
						}
					}
				}(&votes, i)
			}
			time.Sleep(3 * time.Millisecond)
			rf.mu.Lock()
			electionWon := votes >= majority
			rf.mu.Unlock()
			if electionWon {
				if rf.getElectionTimeout() <= 0 {
					rf.mu.Lock()
					rf.IsLeader = true
					rf.ElectionTimeOut = GetRandomElectionTimeout()
					rf.mu.Unlock()
					go rf.ImmediateHeartBeat()
					fmt.Println(rf.me, " is the Leader...")
				}

			} else {
				// If election loses then do nothing and wait for timeout for next election
				// Do not reset the timer -- This is wrong!
				rf.resetElectionTimeout()
			}
		}
	}

}

func (rf *Raft) getElectionTimeout() int {
	rf.mu.Lock()
	timeout := rf.ElectionTimeOut
	rf.mu.Unlock()
	return timeout
}

func (rf *Raft) resetElectionTimeout() {
	rf.mu.Lock()
	rf.ElectionTimeOut = GetRandomElectionTimeout()
	rf.mu.Unlock()
}
