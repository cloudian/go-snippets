package main

import (
	"fmt"
	"os"
	"os/exec"
	"strings"
	"sync"
	"time"
)

var cv *sync.Cond
var mu sync.Mutex
var wg sync.WaitGroup
var ready bool = false

func Runner(cmd string, arg string) {
	defer wg.Done()
	com := strings.Split(cmd, " ")
    com[len(arg)-1] = fmt.Sprintf("%s%s", com[len(com)-1], arg)
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
		fmt.Fprintf(os.Stderr, "Usage: %s \"cmd args\" \"varargs\"", os.Args[0])
		return
	}

	cv = sync.NewCond(&mu)
    varargs := strings.Split(os.Args[2], " ")

    for _, arg := range varargs {
		wg.Add(1)
		go Runner(os.Args[1], arg)
    }

	ready = true
	cv.Broadcast()
	wg.Wait()
}
