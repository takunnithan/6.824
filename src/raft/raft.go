package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, Term, isleader)
//   start agreement on a new log entry
// rf.GetState() (Term, isLeader)
//   ask a Raft for its current Term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"fmt"
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

const (
	Follower  = "follower"
	Candidate = "candidate"
	Leader    = "leader"
	Stopped   = "stopped"
	Begin     = "begin"
)

const HeartbeatTimeInterval = 10

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu                sync.Mutex          // Lock to protect shared access to this peer's state
	peers             []*labrpc.ClientEnd // RPC end points of all peers
	persister         *Persister          // Object to hold this peer's persisted state
	me                int                 // this peer's index into peers[]
	state             string
	eventCh           chan Event
	stopHeartbeat	  chan struct{}
	stopElectionCh	  chan struct{}

	// Move these to LogEntry struct
	//-------
	log               []Log
	commitIndex       int
	lastApplied       int
	//---------

	currentTerm       int
	votedFor          int
	nextIndex         map[int]int

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

func (rf *Raft) SendEvents(event Event) {
	rf.eventCh <- event
}

func (rf *Raft) getState() string {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	state := rf.state
	return state
}

func (rf *Raft) setState(state string) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.state = state
}

func (rf *Raft) getCurrentTerm() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm
}

func (rf *Raft) setCurrentTerm(term int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.currentTerm = term
}

func (rf *Raft) getVotedFor() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.votedFor
}

func (rf *Raft) getLogs() []Log {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.log
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	term := rf.getCurrentTerm()
	isLeader := false
	if rf.getState() == Leader {
		isLeader = true
	}
	return term, isLeader
}

func (rf *Raft) GetPeers() []*labrpc.ClientEnd {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.peers
}

func (rf *Raft) getLastLogIndex() int {
	logs := rf.getLogs()
	return len(logs) - 1
}

func (rf *Raft) getLastLogTerm() int {
	logs := rf.getLogs()
	if len(logs) == 0 {
		return -1
	}
	lastLogTerm := logs[len(logs)-1].Term
	return lastLogTerm
}

func (rf *Raft) getNextIndex(peerIndex int) int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.nextIndex[peerIndex]
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

func (rf *Raft) getRequestVoteArgs() RequestVoteArgs {
	requestVoteArgs := RequestVoteArgs{
		Term:         rf.getCurrentTerm() + 1,
		CandidateId:  rf.me,
		LastLogIndex: rf.getLastLogIndex(),
		LastLogTerm:  rf.getLastLogTerm(),
	}
	return requestVoteArgs
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	currentTerm := rf.getCurrentTerm()
	votedFor := rf.getVotedFor()
	logs := rf.getLogs()

	if args.Term < currentTerm {
		UpdateRequestVoteResponse(reply, currentTerm, false)
		return
	}

	if len(logs) == 0 {
		UpdateRequestVoteResponse(reply, currentTerm, true)
		return
	}
	followerLastLog := logs[len(logs)-1]
	if votedFor == 0 || votedFor == args.CandidateId {
		if args.LastLogTerm >= followerLastLog.Term {
			if args.LastLogTerm == followerLastLog.Term && args.LastLogIndex < len(logs)-1 {
				UpdateRequestVoteResponse(reply, currentTerm, false)
				return
			}
			UpdateRequestVoteResponse(reply, currentTerm, true)
			return
		} else {
			UpdateRequestVoteResponse(reply, currentTerm, false)
			return
		}
	} else {
		UpdateRequestVoteResponse(reply, currentTerm, false)
		return
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
func (rf *Raft) sendRequestVote(server *labrpc.ClientEnd, args *RequestVoteArgs, reply *RequestVoteReply, ch chan bool) {
	ok := server.Call("Raft.RequestVote", args, reply)
	ch <- ok
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

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	currentTerm := rf.getCurrentTerm()
	logs := rf.getLogs()
	fmt.Println(args)
	if args.Term < currentTerm {
		UpdateAppendEntriesResponse(reply, currentTerm, false)
		return
	}
	if len(logs)-1 < args.PrevLogIndex {
		UpdateAppendEntriesResponse(reply, currentTerm, false)
		return
	} else {
		//PrevLogIndex is -1 because a newly elected leader may not have any logs
		if args.PrevLogIndex != -1 {
			if logs[args.PrevLogIndex].Term != args.PrevLogTerm {
				//rf.log = rf.log[:args.PrevLogIndex]
				UpdateAppendEntriesResponse(reply, currentTerm, false)
				return
			}
		}
	}
	// HeartBeat
	if len(args.Entries) == 0 {
		rf.SendEvents(NewEvent(StateChange, Follower, nil))
		UpdateAppendEntriesResponse(reply, currentTerm, true)
		return
	}
}
func (rf *Raft) sendAppendEntries(peerIndex int, args *AppendEntriesArgs, reply *AppendEntriesReply, ch chan bool)  {
	ok := rf.peers[peerIndex].Call("Raft.AppendEntries", args, reply)
	ch<-ok
	return
}

func (rf *Raft) getAppendEntriesArgs(isHeartBeat bool, peerIndex int) *AppendEntriesArgs {
	// Using next index to get previousLogTerm & Index
	previousLogIndex := -1
	previousLogTerm := -1
	nextIndex := rf.getNextIndex(peerIndex)
	if nextIndex > 0 {
		previousLogIndex = nextIndex - 1
		previousLog := rf.getLogs()[previousLogIndex]
		previousLogTerm = previousLog.Term
	}

	var logEntries []Log
	if !isHeartBeat {
		logEntries = rf.log[nextIndex:]
	}
	args := &AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: previousLogIndex,
		PrevLogTerm:  previousLogTerm,
		Entries:      logEntries,
		LeaderCommit: rf.commitIndex}
	return args
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
// Term. the third return value is true if this server believes it is
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

	// Your initialization code here (2A, 2B, 2C).
	rf.eventCh = make(chan Event)
	rf.stopHeartbeat = make(chan struct{})
	rf.nextIndex = make(map[int]int)
	rf.currentTerm = -1
	rf.commitIndex =-1
	rf.log = []Log{}
	rf.me = me

	rf.setState(Follower)
	go rf.eventLoop()
	go rf.StartElectionTimer()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}

func (rf *Raft) eventLoop() {
	//state :=
	//for state != Stopped {
	for  {
		switch rf.getState() {

		case Follower:
			rf.HandleFollowerState()

		case Candidate:
			rf.HandleCandidateState()

		case Leader:
			rf.HandleLeaderState()

		}
		//state = rf.getState()
	}
}

func (rf *Raft) HandleCandidateState() {
	for {
		select {
		case event := <-rf.eventCh:
			switch event.eventType {
			case StateChange:
				rf.ProcessStateChange(event.req)
				return
			}
		}
	}
}

func (rf *Raft) HandleLeaderState() {
	for {
		select {
		case event := <-rf.eventCh:
			switch event.eventType {
			case StateChange:
				rf.ProcessStateChange(event.req)
				return
			}
		}
	}
}






// May be sending every state change via event loop is a bad idea!!!















func (rf *Raft) HandleFollowerState() {
	for {
		select {
		case event := <-rf.eventCh:
			switch event.eventType {
			case StateChange:
				fmt.Printf("Raft peer: %d --> state change\n", rf.me)
				rf.ProcessStateChange(event.req)
				return
			case ElectionTimeout:
				fmt.Printf("Raft peer: %d --> ElectionTimeout\n", rf.me)
				rf.ProcessElectionTimeout()
				continue
			}
		}
	}
}

func UpdateAppendEntriesResponse(response *AppendEntriesReply, followerTerm int, Success bool) {
	response.Term = followerTerm
	response.Success = Success
}

func UpdateRequestVoteResponse(response *RequestVoteReply, followerTerm int, voteGranted bool) {
	response.Term = followerTerm
	response.VoteGranted = voteGranted
}


func (rf *Raft) ProcessStateChange(req interface{}) {
	state := rf.getState()
	newState := req.(string)
	if state == Leader {
		rf.stopHeartbeat <- struct{}{}
	}
	if state == Follower {
	}
	if state == Candidate {
		// Stop election
		//rf.stopElectionCh <- struct{}{}
	}

	if newState == Follower || newState == Candidate {
		go rf.StartElectionTimer()
	}

	if newState == Candidate {
		fmt.Printf("Raft peer: %d, I am a candidate\n", rf.me)
		go rf.RunElection()
	}

	if newState == Leader {
		fmt.Println("I am LEADERERRR")
		// Start sending out heartbeats
		go rf.heartBeat()
	}

	rf.setState(newState)
}

func (rf *Raft) ProcessElectionTimeout() {
	go rf.SendEvents(NewEvent(StateChange, Candidate, nil))
}

func (rf *Raft) StartElectionTimer() {
	select {
	case <-time.After(time.Duration(getRandomTimeout()) * time.Millisecond):
		go rf.SendEvents(NewEvent(ElectionTimeout, nil, nil))
	case <-rf.stopElectionCh:
		return
	}
}

func (rf *Raft) RunElection() {
	otherPeers := rf.getOtherPeers()
	voteGranted := 1
	args := rf.getRequestVoteArgs()
	rf.setCurrentTerm(rf.getCurrentTerm() + 1)
	electionTerm := rf.getCurrentTerm()
	var wg sync.WaitGroup
	wg.Add(len(otherPeers))
	for _, peerIndex := range otherPeers {
		go func(rf *Raft, peerIndex int, args *RequestVoteArgs, voteGranted *int, wg *sync.WaitGroup) {
			reply := &RequestVoteReply{}
			ch := make(chan bool)
			peer := rf.GetPeers()[peerIndex]
			fmt.Printf("Raft peer %d: -- sending vote request to peer - %d\n", rf.me, peerIndex)
			go rf.sendRequestVote(peer, args, reply, ch)
			select {
			case res := <-ch:
				if res == true {
					if reply.VoteGranted == true {
						*voteGranted = *voteGranted + 1
					} else {
						if reply.Term > electionTerm {
							rf.setCurrentTerm(reply.Term)
							// Stop election at this point and become a follower
							return
						}
					}
				}
				wg.Done()
			case <-time.After(2 * time.Millisecond):
				wg.Done()
				return
			}
		}(rf, peerIndex, &args, &voteGranted, &wg)
	}

	wg.Wait()
	majority := GetMajority(len(rf.peers))
	//rfElectionTimer := rf.electionTimer
	//if rfElectionTimer > 0 {	-- If election timeout before election completion
	//if rfCurrentTerm == electionTerm {  -- If currentTerm changes during election
	//- Ideally election should stop if someone else becomes a leader
	if voteGranted >= majority {
		fmt.Println("I AM THE LEADER", rf.me, "majority: ", majority, "Votes: ", voteGranted)
		go rf.SendEvents(NewEvent(StateChange, Leader, nil))
		//rf.setCurrentTerm(rfCurrentTerm)
	}

}

func (rf *Raft) getOtherPeers() []int {
	peers := rf.GetPeers()
	var otherPeers []int
	for index := range peers {
		if index != rf.me {
			otherPeers = append(otherPeers, index)
		}
	}
	return otherPeers
}

func (rf *Raft) heartBeat() {
	otherPeers := rf.getOtherPeers()
	rf.sendHeartBeat(otherPeers)
	ticker := time.NewTicker(time.Duration(HeartbeatTimeInterval) * time.Millisecond)
	for {
		select {
		case <-ticker.C:
			rf.sendHeartBeat(otherPeers)
		case <-rf.stopHeartbeat:
			ticker.Stop()
			return
		}
	}
}

func (rf *Raft) sendHeartBeat(otherPeers []int) {
	fmt.Printf("Raft Peer: %d, ----Sending HeartBeats", rf.me)
	for _, peerIndex := range otherPeers {
		reply := &AppendEntriesReply{}
		go func(peerIndex int, reply *AppendEntriesReply) {
			args := rf.getAppendEntriesArgs(true, peerIndex)
			ch := make(chan bool)
			go rf.sendAppendEntries(peerIndex, args, reply, ch)
			select {
			case res := <-ch:
				if res {
					if reply.Term > rf.currentTerm {
						rf.SendEvents(NewEvent(StateChange, Follower, nil))
						rf.setCurrentTerm(reply.Term)
					}
				}
				return

			case <-time.After(2 * time.Millisecond):
				return
			}
		}(peerIndex, reply)
	}
}
