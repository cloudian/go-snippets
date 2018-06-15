package main

import (
	"flag"
	"fmt"
	"net/rpc"
	"os"
	"time"
)

type Args struct {
	Message string
}

type Result struct {
	Result string
}

var msg = flag.String("m", "test", "The message to transform to upper case")

func main() {
	flag.Parse()
	client, err := rpc.Dial("tcp", ":1234")
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		return
	}

	args := Args{Message: *msg}
	result := Result{}
	serviceCall := client.Go("NetService.ToUpper", args, &result, nil)
	select {
	case <-time.After(time.Second * 1):
		fmt.Println("Timeout")
	case reply := <-serviceCall.Done:
		if reply != nil {
            if reply.Error == nil {
                fmt.Println("Reply:", reply.Reply.(*Result).Result)
            } else {
                fmt.Fprintf(os.Stderr, "Err: %v\n", reply.Error)
            }
		} else {
			fmt.Println("Error")
		}
	}
}
