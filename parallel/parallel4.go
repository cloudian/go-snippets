package main

import (
	"fmt"
	"os"
	"os/exec"
	"strings"
	"sync"
	"time"
    "flag"
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
		fmt.Fprintf(os.Stderr, "Error running %s %v\n", cmd, err)
	}
}

var help = flag.Bool("h", false, "Print help message")
var delim = flag.String("d", " ", "Delimiter to split variable command line arguments")
var command = flag.String("c", "", "Command to execute")
var vargs = flag.String("v", "", "list of arguments separated by delimiter")

func main() {
    flag.Parse()

    if *help {
        fmt.Printf("Use %s -d <delimiter> ", os.Args[0])
        fmt.Printf(" -c <command> -v \"list of arguments\"")
        fmt.Printf(" separated by <delimiter>\n") 
        fmt.Println("Example:")
        fmt.Printf("\t%s -c ", os.Args[0])
        fmt.Print("\"dd if=/dev/urandom of=%s bs=1m count=5\" \\\n\t-d \"#\"")
        fmt.Print(" -v \"/tmp/file1#/tmp/file2#/tmp/file3\"\n")
        return
    }

    if *delim == "" {
        fmt.Println("You need to use -d <delimiter>, e.g. -d ;")
        return
    }

    if *command == "" {
        fmt.Println("You need to use -c <command>, e.g. -c \"mkfs.ext4fs -balh -blub %s\"")
        return
    }

    if *vargs == "" {
        fmt.Println("You need to use -v <list of arguments used in -c>")
        return
    }

	cv = sync.NewCond(&mu)
    varargs := strings.Split(*vargs, *delim)

    for _, arg := range varargs {
		wg.Add(1)
        cmd := fmt.Sprintf(*command, arg)
        fmt.Println(cmd)
		go Runner(cmd)
    }

	ready = true
	cv.Broadcast()
	wg.Wait()
}
