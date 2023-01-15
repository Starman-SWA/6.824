package raft

import "log"

// Debugging
// standard info including every function call
const Debug = false

// extra debug info
const Debug1 = false

// print mutex info
const Debug2 = false

// for lab3
const Debug3 = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

func DPrintf1(format string, a ...interface{}) (n int, err error) {
	if Debug1 {
		log.Printf(format, a...)
	}
	return
}

func DPrintf2(format string, a ...interface{}) (n int, err error) {
	if Debug2 {
		log.Printf(format, a...)
	}
	return
}

func DPrintf3(format string, a ...interface{}) (n int, err error) {
	if Debug3 {
		log.Printf(format, a...)
	}
	return
}
