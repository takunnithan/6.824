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
	Command interface{}
	Term    int
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
	votedFor         map[int]int
	log              []Log
	commitIndex      int
	lastAppliedIndex int
	nextIndex        map[int]int
	matchIndex       []MatchIndex
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	electionTimer int
	state         string
	otherServers  []int
	applyCh chan ApplyMsg
	appendInProgress bool
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	term = rf.currentTerm
	isleader = rf.state == LEADER
	rf.mu.Unlock()
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
	Success     bool
}

func (rf *Raft) acceptVoteRequest(args *RequestVoteArgs, reply *RequestVoteReply) {
	reply.Term = args.Term
	reply.VoteGranted = true

	// Updating rf
	rf.mu.Lock() //-------->
	rf.votedFor[args.Term] = args.CandidateId
	rf.currentTerm = args.Term
	rf.state = FOLLOWER
	rf.electionTimer = GetRandomElectionTimeout()
	rf.mu.Unlock() //------->
}

func (rf *Raft) rejectVoteRequest(reply *RequestVoteReply) {
	rf.mu.Lock()
	reply.Term = rf.currentTerm
	rf.mu.Unlock()
	reply.VoteGranted = false
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {

	// Your code here (2A, 2B).
	rf.mu.Lock()
	currentTerm := rf.currentTerm
	rf.mu.Unlock()
	if args.Term < currentTerm {
		reply.Term = currentTerm
		reply.VoteGranted = false
		return
	}
	// Terrible Check -- Need Changes
	rf.mu.Lock() //-------------->
	votedFor := rf.votedFor
	rf.mu.Unlock() //---------------->
	if votedFor[args.Term] == args.CandidateId || votedFor[args.Term] == 0 {
		rf.mu.Lock() //--------->
		log := rf.log
		rf.mu.Unlock()
		if len(log) > 0 { // No logs
			followerLastLog := log[len(log)-1]
			if args.LastLogTerm >= followerLastLog.Term {
				if args.LastLogTerm == followerLastLog.Term && args.LastLogIndex < len(log)-1 {
					rf.rejectVoteRequest(reply)
					return
				}
				rf.acceptVoteRequest(args, reply)
				return
			} else {
				rf.rejectVoteRequest(reply)
				return
			}
		} else {
			rf.acceptVoteRequest(args, reply)
			return
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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply, ch chan bool) {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	ch <- ok
}

func (rf *Raft) getRequestVoteArgs() *RequestVoteArgs {
	LastLogIndex := -1
	LastLogTerm := -1
	if len(rf.log) > 0 {
		LastLogIndex = len(rf.log) - 1
		LastLogTerm = rf.log[LastLogIndex].Term
	}
	return &RequestVoteArgs{
		Term:         rf.currentTerm + 1,
		CandidateId:  rf.me,
		LastLogIndex: LastLogIndex,
		LastLogTerm:  LastLogTerm}
}

func (rf *Raft) HandleElection() {
	majority := GetMajority(len(rf.peers))
	for {
		rf.mu.Lock() //--------->
		currentState := rf.state
		rf.mu.Unlock() //---------->
		if currentState == CANDIDATE {
			rf.mu.Lock() // ---->

			electionTerm := rf.currentTerm + 1
			rf.electionTimer = GetRandomElectionTimeout()
			rf.mu.Unlock() // ---->
			args := rf.getRequestVoteArgs()
			voteGranted := 1
			var wg sync.WaitGroup
			wg.Add(len(rf.otherServers))
			for _, server := range rf.otherServers {
				go func(rf *Raft, server int, args *RequestVoteArgs, voteGranted *int, wg *sync.WaitGroup) {
					reply := &RequestVoteReply{}
					ch := make(chan bool, 1)
					go rf.sendRequestVote(server, args, reply, ch)
					select {
					case res := <-ch:
						if res {
							if reply.VoteGranted {
								*voteGranted = *voteGranted + 1 // Potential data race
							} else {
								if reply.Term > electionTerm {
									rf.mu.Lock() // ---->
									rf.currentTerm = reply.Term
									rf.mu.Unlock() // ---->
								}
							}
						}
						wg.Done()

					case <-time.After(1 * time.Millisecond):
						wg.Done()
						return
					}
				}(rf, server, args, &voteGranted, &wg)
			}
			wg.Wait()    // Hopefully RPC Call() will have a timeout (smaller than election tmout)
			rf.mu.Lock() //---->
			rfElectionTimer := rf.electionTimer
			rfCurrentTerm := rf.currentTerm + 1
			rf.mu.Unlock()
			if rfElectionTimer > 0 {
				if rfCurrentTerm == electionTerm {
					if voteGranted >= majority {
						rf.mu.Lock()
						rf.state = LEADER
						rf.currentTerm = rfCurrentTerm
						rf.mu.Unlock()
						rf.setNextIndex()
						rf.HeartBeat(true)
						fmt.Println("I AM THE LEADER", rf.me, "majority: ", majority, "Votes: ", voteGranted)
					}
				}
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func (rf *Raft) setNextIndex() {
	rf.mu.Lock()
	for _, server := range rf.otherServers {
		rf.nextIndex[server] = len(rf.log)
	}
	rf.mu.Unlock()
}

func (rf *Raft) convertToFollower() {
	rf.state = FOLLOWER
	rf.electionTimer = GetRandomElectionTimeout()
}

func (rf *Raft) convertToCandidate() {
	for {
		rf.mu.Lock()
		if rf.electionTimer == 0 && rf.state != CANDIDATE {
			rf.state = CANDIDATE
			fmt.Println(rf.me, " has become a candidate")
		}
		rf.mu.Unlock()
		time.Sleep(5 * time.Microsecond)
	}
}

func (rf *Raft) ElectionTimerCounter() {
	for {
		rf.mu.Lock()
		if rf.electionTimer > 0 && rf.state != LEADER {
			rf.electionTimer = rf.electionTimer - 1
		}
		rf.mu.Unlock()
		time.Sleep(1 * time.Millisecond)
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply, ch chan bool) {
	//rf.mu.Lock() //------->
	peer := rf.peers[server]
	//rf.mu.Unlock() //------->
	ok := peer.Call("Raft.AppendEntries", args, reply)
	ch <- ok
}

func (rf *Raft) getAppendEntriesArgs(isHeartBeat bool, server int) *AppendEntriesArgs {

	// Using next index to get previousLogTerm & Index
	previousLogIndex := -1
	previousLogTerm := -1
	nextIndex := rf.nextIndex[server]
	if nextIndex > 0 {
		previousLogIndex = nextIndex - 1
		previousLog := rf.log[previousLogIndex]
		previousLogTerm = previousLog.Term
	}

	var logEntries []Log
	if !isHeartBeat {
		logEntries = rf.log[nextIndex:]
		fmt.Println("\n From getAppEntries -- server: ", server, "logs: ", rf.log, "nextIndex: ", nextIndex, "selected logs: ", logEntries)
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

// Does this send heartbeats immediately , when a candidate becomes a leader  ????
func (rf *Raft) HeartBeat(immediate bool) {
	for {
		rf.mu.Lock() //---->
		currentState := rf.state
		rf.mu.Unlock() //---->
		if currentState == LEADER {
			reply := &AppendEntriesReply{}
			for _, server := range rf.otherServers {
				args := rf.getAppendEntriesArgs(true, server)
				go func(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
					ch := make(chan bool, 1)
					rf.sendAppendEntries(server, args, reply, ch)
					select {
					case res := <-ch:
						if res {
							rf.mu.Lock() //---------->
							if reply.Term > rf.currentTerm {
								rf.currentTerm = reply.Term
								rf.state = FOLLOWER
							}
							rf.mu.Unlock() //---------->
							break
						}

					case <-time.After(1 * time.Millisecond):
						return
					}
				}(server, args, reply)
			}
			time.Sleep(10 * time.Millisecond)
		}
		if immediate {
			return
		}
		time.Sleep(2 * time.Microsecond)
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Check if it is a heartbeat or regular append entries RPC
	// check the conditions
	rf.mu.Lock() //----->
	currentTerm := rf.currentTerm
	rf.mu.Unlock()
	if args.Term < currentTerm {
		reply.Success = false
		reply.Term = currentTerm
		fmt.Println("Small term -- server: ", rf.me, ":   argsTerm: ", args.Term, "server Term: ", rf.currentTerm, " args: ", args)
		return
	} else {
		rf.mu.Lock()
		rf.currentTerm = args.Term
		reply.Term = rf.currentTerm
		rf.mu.Unlock()
	}

	// Only reset election timer for heart beats
	if len(rf.log)-1 < args.PrevLogIndex {
		reply.Success = false
		reply.Term = currentTerm
		return
	} else {
		if args.PrevLogIndex != -1 {
			if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
				rf.mu.Lock()
				rf.log = rf.log[:args.PrevLogIndex]
				reply.Success = false
				reply.Term = rf.currentTerm
				rf.mu.Unlock()
				return
			}

		}
	}

	// HeartBeat
	if len(args.Entries) == 0 {
		rf.mu.Lock() //----->
		rf.convertToFollower()
		reply.Term = rf.currentTerm
		rf.updateCommitIndexAndSendApplyMsg(args)
		rf.mu.Unlock() //------------->
		reply.Success = true
		return
	}

	rf.mu.Lock() //----->
	if args.PrevLogIndex != -1 {
		rf.log = rf.log[:args.PrevLogIndex+1]	// `+1` because end `]` is exclusive
	} else {
		rf.log = []Log{}
	}
	for _, logEntry := range args.Entries {
		rf.log = append(rf.log, logEntry)

		// =================================================

		// TODO: When to send applyMsg
		//  Should the leader send log applied message ?
		//  CommitIndex & ApplyMsg
		//  Read the paper & Guides by jon Gjengset
		//  Pay more attentions to the details
		// =================================================

		// Sending index = len(rf.log)  => since log index starts from 1
		//rf.applyCh <- ApplyMsg{CommandValid: true, Command: logEntry.Command, CommandIndex:len(rf.log)}
	}
	fmt.Println("server: ", rf.me, " Log: ", rf.log)
	rf.convertToFollower()
	reply.Term = rf.currentTerm
	rf.updateCommitIndexAndSendApplyMsg(args)

	rf.mu.Unlock() //------------->
	reply.Success = true
	return
}

 func (rf *Raft) updateCommitIndexAndSendApplyMsg(args *AppendEntriesArgs){
	 previousCommitIndex := rf.commitIndex
	 if args.LeaderCommit > rf.commitIndex {
		 // There is no min() function for integers
		 //if args.LeaderCommit > rf.commitIndex {		---- Last new entry - means last entry
		 if args.LeaderCommit > len(rf.log) - 1 {
			 rf.commitIndex = len(rf.log) - 1
		 } else {
			 rf.commitIndex = args.LeaderCommit
		 }
	 }
	 latestCommitIndex := rf.commitIndex
	 fmt.Println("Update Commit: server: ", rf.me, "prev comm index: ", previousCommitIndex, "Latest C Ind: ", latestCommitIndex)
	 for i:=previousCommitIndex+1; i<= latestCommitIndex; i++ {
		 fmt.Println("Committing: server: ", rf.me, " i: ", i, "command: ", rf.log[i].Command)

		 // `i+1` since client expect commandIndex to start from 1
		 rf.applyCh <- ApplyMsg{CommandValid: true, Command: rf.log[i].Command, CommandIndex:i+1}
	 }
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
	// Your code here (2B).
	for {
		//=====================================
		// TODO:
		//	This doesn't handle concurrent append requests from clients -> Queue / reject ???
		//=====================================
		rf.mu.Lock()
		progress := rf.appendInProgress
		rf.mu.Unlock()
		if progress {
			fmt.Println("Another Append is in progress, waiting ....")
			fmt.Println("Waiting command: ", command)
			time.Sleep(10 * time.Millisecond)
		} else {
			fmt.Println("Continue with append: ", command)
			break
		}
	}

	fmt.Println("START COMMAND : ", command, " from: ", rf.me)
	rf.mu.Lock()
	term := rf.currentTerm
	// This is the index where the new entry will be added to
	index := len(rf.log) + 1	// Log index starts from `1` for the clients
	isLeader := rf.state == LEADER
	rf.mu.Unlock()
	if !isLeader {
		fmt.Println("Not leader: ", rf.me)
		return index, term, isLeader
	}
	rf.mu.Lock()
	rf.appendInProgress = true
	rf.mu.Unlock()

	go func(rf *Raft) {
		rf.mu.Lock()
		rf.log = append(rf.log, Log{Command: command, Term: rf.currentTerm})
		fmt.Println("Log Appended: ", rf.log)
		replicationCounter := 1   // self-replication ???
		var wg sync.WaitGroup
		for _, server := range rf.otherServers {
			appendEntriesArgs := rf.getAppendEntriesArgs(false, server)
			wg.Add(1)
			go func(server int, rf *Raft, wg *sync.WaitGroup, replicationCounter *int) {
				ch := make(chan bool, 1)
				reply := &AppendEntriesReply{}
				fmt.Println("Sending RPC to : ", server)
				go rf.sendAppendEntries(server, appendEntriesArgs, reply, ch)
				timer := time.NewTimer(5 * time.Millisecond)
				select {
				case res := <-ch:
					if res {
						if reply.Success {
							*replicationCounter = *replicationCounter + 1
							rf.nextIndex[server] ++		// Should this be run after commit
							fmt.Printf("\nAppend Success server: %d, leader: %d, nextIndex: %d \n", server, rf.me, rf.nextIndex[server])
						} else {
							fmt.Println("Append FAILED..........")
							//------------------------------------------------------------
							//							Check again if this necessary

							if reply.Term > rf.currentTerm {
								rf.currentTerm = reply.Term
								rf.convertToFollower()
							} else {
								rf.updateFollowerLogs(server)
							}
							// -------------------------------------------------------

							//rf.updateFollowerLogs(server)
						}

					}
					timer.Stop()
					wg.Done()
				case <-timer.C:
					wg.Done()
				}
			}(server, rf, &wg, &replicationCounter)
		}
		wg.Wait()
		majority := GetMajority(len(rf.peers))
		if replicationCounter >= majority && rf.state == LEADER {
			//rf.mu.Lock()
			// ===================================================================================

			// TODO: The increment is based on how many entries were committed - > Not `+1`
			//	1. Scenario 1:
			//	- Append failed for command `20`. but the next command succeeded - `30`
			//	- We need to ApplyMsg both - `20` & `30` with their respective `CommandIndex`
			//	- for eg: ` ApplyMsg(true, 20, 2) , ApplyMsg(true, 30, 3)
			//	- Check the log, find the last commit index, latest commit index - apply all in between

			// ===================================================================================
			rf.commitIndex = 1 + rf.commitIndex
			fmt.Println("Leader Increased commit index: ", rf.commitIndex)
			//rf.mu.Unlock()
			fmt.Println("server--: ", rf.me, " Log: ", rf.log)
			rf.applyCh <- ApplyMsg{CommandValid: true, Command: command, CommandIndex:index}
		//} else {
		//	// Remove the newly appended entry | don't insert the new entry until majority replicates it
		//	fmt.Println(">>>>>>>>>>>>>>>>> : ", index-2)
		//	rf.log = rf.log[:index-2]
		//
			// TODO: Find out if master removes un successful entries ?????????????????
		}
		rf.appendInProgress = false
		rf.mu.Unlock()
		rf.HeartBeat(true)
	}(rf)

	//rf.mu.Lock()
	//rf.log = append(rf.log, Log{Command: command, Term: rf.currentTerm})
	//fmt.Println("Log Appended: ", rf.log)
	////rf.mu.Unlock()
	//replicationCounter := 1   // self-replication ???
	//var wg sync.WaitGroup
	//for _, server := range rf.otherServers {
	//	appendEntriesArgs := rf.getAppendEntriesArgs(false, server)
	//	wg.Add(1)
	//	go func(server int, rf *Raft, wg *sync.WaitGroup, replicationCounter *int) {
	//		ch := make(chan bool, 1)
	//		reply := &AppendEntriesReply{}
	//		fmt.Println("Sending RPC to : ", server)
	//		go rf.sendAppendEntries(server, appendEntriesArgs, reply, ch)
	//		timer := time.NewTimer(5 * time.Millisecond)
	//		select {
	//		case res := <-ch:
	//			if res {
	//				if reply.Success {
	//					*replicationCounter = *replicationCounter + 1
	//					//rf.mu.Lock()
	//					rf.nextIndex[server] ++
	//					//rf.mu.Unlock()
	//					fmt.Printf("\nAppend Success server: %d, leader: %d, nextIndex: %d \n", server, rf.me, rf.nextIndex[server])
	//				} else {
	//					fmt.Println("Append FAILED..........")
	//					//------------------------------------------------------------
	//					//							Check again if this necessary
	//
	//					//if reply.Term > rf.currentTerm {
	//					//	rf.currentTerm = reply.Term
	//					//	rf.convertToFollower()
	//					//} else {
	//					//	rf.updateFollowerLogs(server)
	//					//}
	//					// --------------------------------------------------------
	//					rf.updateFollowerLogs(server)
	//					//go rf.updateFollowerLogs(server)
	//				}
	//
	//			}
	//			timer.Stop()
	//			wg.Done()
	//		case <-timer.C:
	//			wg.Done()
	//		}
	//	}(server, rf, &wg, &replicationCounter)
	//}
	//wg.Wait()
	//majority := GetMajority(len(rf.peers))
	//if replicationCounter >= majority && rf.state == LEADER {
	//	//rf.mu.Lock()
	//	rf.commitIndex = 1 + rf.commitIndex
	//	fmt.Println("Leader Increased commit index: ", rf.commitIndex)
	//	//rf.mu.Unlock()
	//	fmt.Println("server--: ", rf.me, " Log: ", rf.log)
	//	rf.applyCh <- ApplyMsg{CommandValid: true, Command: command, CommandIndex:index}
	////} else {
	////	// Remove the newly appended entry | don't insert the new entry until majority replicates it
	////	fmt.Println(">>>>>>>>>>>>>>>>> : ", index-2)
	////	rf.log = rf.log[:index-2]
	//
	//
	//	// TODO: Find out if master removes un successful entries ?????????????????
	//
	//
	//}
	//rf.mu.Unlock()
	// Here returning index ( since logs should start from index 1)
	return index, term, isLeader
}

func (rf *Raft) updateFollowerLogs(server int) {
	fmt.Println("Updating follower logs")
	for {
		//rf.mu.Lock()
		if rf.nextIndex[server] > 0 {
			rf.nextIndex[server] = rf.nextIndex[server] - 1
		}
		//rf.mu.Unlock()
		appendEntriesArgs := rf.getAppendEntriesArgs(false, server)
		reply := &AppendEntriesReply{}
		ch := make(chan bool)
		fmt.Println("From updateFollowerLogs-- server: ", server, "appendEntries: ", appendEntriesArgs)
		go rf.sendAppendEntries(server, appendEntriesArgs, reply, ch)
		select {
		case res := <-ch:
			if res {
				if reply.Success {
					//rf.mu.Lock()
					rf.nextIndex[server] ++
					//rf.mu.Unlock()
					return
				}
			}
		case <-time.After(2 * time.Millisecond):	// This could turn into a eternal loop
			continue
		}
	}
}

func (rf *Raft) updateLastApplied() {
	for {
		rf.mu.Lock()
		if rf.commitIndex > rf.lastAppliedIndex {
			rf.lastAppliedIndex = rf.commitIndex
		}
		rf.mu.Unlock()
		time.Sleep(2 * time.Millisecond)
	}

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
	rf.currentTerm = 0
	rf.commitIndex = -1
	rf.votedFor = make(map[int]int)
	rf.nextIndex = make(map[int]int)
	rf.state = FOLLOWER
	rf.electionTimer = GetRandomElectionTimeout()
	rf.applyCh = applyCh
	rf.lastAppliedIndex = -1			// - can't be `0`.
	rf.appendInProgress = false


	rf.log = []Log{}
	fmt.Println(rf.electionTimer)

	// Your initialization code here (2A, 2B, 2C).
	rf.getThePeers()
	go rf.ElectionTimerCounter()
	go rf.convertToCandidate()
	go rf.HandleElection()
	go rf.HeartBeat(false)
	go rf.updateLastApplied()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
