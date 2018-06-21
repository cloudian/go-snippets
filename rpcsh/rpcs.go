package main

import (
	"bytes"
	"errors"
	"fmt"
	"net"
	"net/rpc"
	"os"
	"os/exec"
)

type CmdService int

type Args struct {
	Id   int64
	Argv []string
}

type Result struct {
	Cmd    string
	Id     int64
	Stdout string
	Stderr string
	err    error
}

func (t *CmdService) RunCommand(args *Args, result *Result) error {
	if len(args.Argv) == 0 {
		return errors.New("To few arguments")
	}

	p, err := exec.LookPath(args.Argv[0])
	if err != nil {
		return err
	}
	fmt.Printf("%v\n", p)
	cmd := exec.Command(p, args.Argv[1:]...)
	r := Result{}
	r.Cmd = p
	r.Id = args.Id
	outs := new(bytes.Buffer)
	errs := new(bytes.Buffer)
	cmd.Stdout = outs
	cmd.Stderr = errs
	err = cmd.Run()
	if err != nil {
		return err
	}

	r.Stdout = outs.String()
	r.Stderr = errs.String()

	return nil
}

func main() {
	ns := new(CmdService)
	rpc.Register(ns)
	l, err := net.Listen("tcp", ":9999")
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		return
	}

	rpc.Accept(l)
}
