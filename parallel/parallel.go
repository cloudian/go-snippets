package main

import (
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"time"
)

var cv *sync.Cond
var mu sync.Mutex
var wg sync.WaitGroup
var ready bool = false

func Runner(cmd string) {
	defer wg.Done()
	com := strings.Split(cmd, " ")
	c := exec.Command(com[0], com[1:]...)
	c.Stdout = os.Stdout

	mu.Lock()
	for ready != true {
		cv.Wait()
	}

	fmt.Println("Runner woke up", cmd, time.Now().UnixNano())
	mu.Unlock()
	err := c.Run()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error running %s %v\n", com[0], err)
		fmt.Fprintf(os.Stderr, "Args %v\n", c.Args)
	}
}

func main() {
	if len(os.Args) < 3 {
		fmt.Fprintf(os.Stderr, "Usage: %s cmd count", os.Args[0])
		return
	}

	cv = sync.NewCond(&mu)
	p, err := strconv.ParseInt(os.Args[2], 10, 64)
	if err != nil {
		fmt.Fprintf(os.Stderr, "<count> is supposed to be a number.")
		return
	}

	var i int64
	for i = 0; i < p; i++ {
		wg.Add(1)
		go Runner(os.Args[1])
	}

	ready = true
	cv.Broadcast()
	wg.Wait()
}
