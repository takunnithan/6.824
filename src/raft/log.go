package raft

import "sync"

type Log struct {
	mu sync.Mutex
	command interface{}
	term int
}

func (l *Log) getCommand() interface{}{
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.command
}

func (l *Log) getLogTerm() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.term
}


//
//type LogEntries struct {
//	l
//}
