package raft

import (
	"log"
	"math/rand"
	"time"
)

// Debugging
const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

func getRandomTimeout() int {
	rand.Seed(time.Now().UnixNano())
	return rand.Intn(150) + 150
}