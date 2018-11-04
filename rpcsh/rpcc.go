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
	"regexp"
	"strings"
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
	dcFilter := strings.Split(os.ExpandEnv("${RPCSH_DC_FILTER}"), ",")
	regionFilter := strings.Split(os.ExpandEnv("${RPCSH_REGION_FILTER}"), ",")
	ipFilter := strings.Split(os.ExpandEnv("${RPCSH_IP_EXCLUDE}"), ",")
	ipOne := os.ExpandEnv("${RPCSH_IP_ONLY}")
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

			if ipOne != "" {
				if record[2] == ipOne {
					ips = append(ips, record[2])
					return ips
				}
			}

			if len(regionFilter) != 0 {
				for i := range regionFilter {
					if record[0] == strings.Trim(regionFilter[i], " \t\n") {
						continue
					}
				}
			}

			if len(dcFilter) != 0 {
				for i := range dcFilter {
					if record[3] == strings.Trim(dcFilter[i], " \t\n") {
						continue
					}
				}
			}

			if len(ipFilter) != 0 {
				for i := range ipFilter {
					if record[2] == strings.Trim(ipFilter[i], " \t\n") {
						continue
					}
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
	if len(os.Args) == 1 {
		fmt.Println("all - executes args in parallel on all nodes")
		fmt.Println("all_wait - executes args one by one on all nodes")
		fmt.Println("set RPCSH_DEBUG to get more verbose output")
		fmt.Println("set RPCSH_DC_FILTER (comma seperated list of DC names to exclude)")
		fmt.Println("set RPCSH_REGION_FILTER (comma seperated list of regions to exclude)")
		fmt.Println("set RPCSH_IP_EXCLUDE (comma seperated list of ip addresses to exclude)")
		fmt.Println("set RPCSH_IP_ONLY to run only on the host with this ip address")
		return
	}

	ipFile := os.ExpandEnv("${RPCSH_IP_FILE}")
	if ipFile == "" {
		ipFile = "/root/CloudianPackages/survey.csv"
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
				re := regexp.MustCompile("[^\\s\n]")
				client, err := rpc.Dial("tcp", fmt.Sprintf("%s:9999", ipaddr))
				if err != nil {
					fmt.Fprintf(os.Stderr, "Connect error: %v ... skipping\n", err)
					return
				}

				args := Args{}
				result := Result{}
				args.Id = rand.Int63()
				if debugOutput == "1" {
					mu.Lock()
					fmt.Fprintf(os.Stdout, "[%s:%d]: '%v'\n", ipaddr, args.Id, os.Args[1:])
					mu.Unlock()
				}
				args.Argv = os.Args[1:]
				serviceCall := client.Go("CmdService.RunCommand", args, &result, nil)
				select {
				case reply := <-serviceCall.Done:
					if reply != nil {
						if reply.Error == nil {
							mu.Lock()
							fmt.Fprintf(os.Stdout, "[%s] %s", ipaddr, reply.Reply.(*Result).Stdout)
							if re.MatchString(reply.Reply.(*Result).Stderr) {
								fmt.Fprintf(os.Stderr, "[%s] %s", ipaddr, reply.Reply.(*Result).Stderr)
							}
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
