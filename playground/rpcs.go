package main

import (
	"errors"
	"fmt"
	"net"
	"net/rpc"
	"os"
	"strings"
)

type NetService int

type Args struct {
	Message string
}

type Result struct {
	Result string
}

func (t *NetService) ToUpper(args *Args, result *Result) error {
    fmt.Println("received call", args.Message)
	if len(args.Message) > 3 {
		result.Result = strings.ToUpper(args.Message)
		return nil
	} else {
		return errors.New("Message too short!")
	}
}

func main() {
	ns := new(NetService)
	rpc.Register(ns)
	l, err := net.Listen("tcp", ":1234")
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		return
	}

	rpc.Accept(l)
}
