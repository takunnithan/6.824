package raft

import (
	"log"
	"math"
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

func GetRandomElectionTimeout() int {
	rand.Seed(time.Now().UnixNano())
	return rand.Intn(100) + 200

}

func GetMajority(noOfServers int) int {
	noOfServer := float64(noOfServers)
	return int(math.Ceil(noOfServer / 2))
}
