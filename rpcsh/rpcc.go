package main

import (
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/rpc"
	"os"
	"path"
	"sync"
)

/* Protect stdout */
var mu sync.Mutex

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

var debugOutput string = os.ExpandEnv("${RPCSH_DEBUG}")

func readIps(fileName string) []string {
	dcFilter := os.ExpandEnv("${RPCSH_DC_FILTER}")
	regionFilter := os.ExpandEnv("${RPCSH_REGION_FILTER}")
	ipFilter := os.ExpandEnv("${RPCSH_IP_EXCLUDE}")
	ips := make([]string, 0)
	if fh, err := os.Open(fileName); err == nil {
		defer fh.Close()
		r := csv.NewReader(fh)
		for {
			record, err := r.Read()
			if err == io.EOF {
				break
			}
			if err != nil {
				log.Printf("error reading line in %s: %v", fileName, err)
				return ips
			}

			if len(record) < 4 {
				log.Printf("error reading line in %s: expecting survey file format", fileName)
				return ips
			}

			if regionFilter != "" {
				if record[0] == regionFilter {
					continue
				}
			}

			if dcFilter != "" {
				if record[3] == dcFilter {
					continue
				}
			}

			if ipFilter != "" {
				if record[2] == ipFilter {
					continue
				}
			}

			ips = append(ips, record[2])
			if debugOutput == "1" {
				fmt.Println("Add ip", record[2], "as command receiver.")
			}
		}
	}
	return ips
}

func main() {
	ipFile := os.ExpandEnv("${RPCSH_IP_FILE}")
	if ipFile == "" {
		ipFile = "/root/cloudian/survey.csv"
	}

	if debugOutput == "1" {
		fmt.Println("Using", ipFile, "to read ip addresses.")
	}

	ips := readIps(ipFile)
	if len(ips) == 0 {
		fmt.Println("No ips found or no ips match your region or dc filter")
		return
	}

	prog := path.Base(os.Args[0])
	if prog == "all" {
		var wg sync.WaitGroup
		for _, ip := range ips {
			wg.Add(1)
			go func(ipaddr string) {
				defer wg.Done()
				client, err := rpc.Dial("tcp", fmt.Sprintf("%s:9999", ipaddr))
				if err != nil {
					fmt.Fprintf(os.Stderr, "Connect error: %v ... skipping\n", err)
					return
				}

				args := Args{}
				result := Result{}
				args.Id = rand.Int63()
				mu.Lock()
				fmt.Fprintf(os.Stdout, "[%s:%d]: '%v'\n", ipaddr, args.Id, os.Args[1:])
				mu.Unlock()
				args.Argv = os.Args[1:]
				serviceCall := client.Go("CmdService.RunCommand", args, &result, nil)
				select {
				case reply := <-serviceCall.Done:
					if reply != nil {
						if reply.Error == nil {
							mu.Lock()
							fmt.Fprintf(os.Stdout, "%s\n", reply.Reply.(*Result).Stdout)
							fmt.Fprintf(os.Stderr, "%s\n", reply.Reply.(*Result).Stderr)
							mu.Unlock()
						} else {
							mu.Lock()
							fmt.Fprintf(os.Stderr, "Err: %s %v\n", ipaddr, reply.Error)
							mu.Unlock()
						}
					} else {
						mu.Lock()
						fmt.Fprintf(os.Stderr, "Err: Never happens %s\n", ip)
						mu.Unlock()
					}
				}
			}(ip)
		}
		wg.Wait()
	}

	if prog == "all_wait" {
		for _, ip := range ips {
			client, err := rpc.Dial("tcp", fmt.Sprintf("%s:9999", ip))
			if err != nil {
				fmt.Fprintf(os.Stderr, "Connect error: %v ... skipping\n", err)
				continue
			}

			args := Args{}
			res := Result{}
			args.Id = rand.Int63()
			fmt.Fprintf(os.Stdout, "[%s:%d]: '%v'\n", ip, args.Id, os.Args[1:])
			args.Argv = os.Args[1:]
			err = client.Call("CmdService.RunCommand", args, &res)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error running command on %s: %v", ip, err)
				return
			}
			fmt.Println(res.Id)
			fmt.Println(res.Stdout, "\n")
			fmt.Fprintf(os.Stderr, "%s\n", res.Stderr)
		}
	}
}
