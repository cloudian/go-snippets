
package main

import (
    "flag"
    "fmt"
    "os"
    "sync"
    "net/rpc"
    "time"
)

type Args struct {
    FileName string
    Offset int64
    FileSize int64
    Blob []byte
}

type BlobInfo struct {
    blobsSend int64
    timeElapsed time.Duration
    mu sync.Mutex
}

type Offsets struct {
    Offsets []uint64
    Pos int
    Mu sync.Mutex
}

var blobInfo BlobInfo = BlobInfo{}
var offset Offsets = Offsets{}

var serverPort = flag.String("h", "127.0.0.1:10002", "server:port to copy to default 127.0.0.1:10001")
var src = flag.String("s", "/usr/share/dict/words", "source file to copy default /usr/share/dict/words")
var dst = flag.String("d", "/var/tmp/file", "destination file on server side default /var/tmp/file")
var cs = flag.Int("c", 4096, "chunk size in bytes, default 4096")
var ngr = flag.Int("n", 1, "number of go routines to copy chunks default 1.")

func main() {
    flag.Parse()
    if fileInfo, err := os.Stat(*src); err != nil {
        fmt.Println("Failed to open", *src, err)
        return
    } else {
        var off uint64 = 0
        offset.Offsets = make([]uint64, 0)
        for off < uint64(fileInfo.Size()) {
            offset.Offsets = append(offset.Offsets, off)
            off += uint64(*cs)
        }

        var waitgroup sync.WaitGroup
        waitgroup.Add(*ngr)
        fmt.Println("Starting", *ngr, "go routines.")
        for i := 0; i < *ngr; i++ {
            go func() {
                defer waitgroup.Done()

                if client, err := rpc.Dial("tcp", *serverPort); err != nil {
                    fmt.Println("Failed to connect:", err)
                    return
                } else {
                    defer client.Close()
                    for {
                        var reply int
                        var args = Args{}
                        args.FileName = *dst
                        args.FileSize = fileInfo.Size()
                        offset.Mu.Lock()
                        if offset.Pos >= len(offset.Offsets) {
                            offset.Mu.Unlock()
                            fmt.Println("Done")
                            break
                        }
                        args.Offset = int64(offset.Offsets[offset.Pos]) 
                        offset.Pos++
                        offset.Mu.Unlock()
                        if fh, err := os.Open(*src); err != nil {
                            fmt.Println("Error reading file:", *src, err)
                            return
                        } else {
                            bytes_to_read := uint64(*cs)
                            if uint64(fileInfo.Size()-args.Offset) < uint64(*cs) {
                                bytes_to_read = uint64(fileInfo.Size()-args.Offset)
                            }

                            args.Blob = make([]byte, bytes_to_read)
                            if _, err := fh.ReadAt(args.Blob, int64(args.Offset)); err != nil {
                                fmt.Println("Failed reading from source:", err)
                                return
                            }

                            fh.Close()
                            start_call := time.Now()
                            if err = client.Call("NetCopy.Put", args, &reply); err != nil {
                                fmt.Println("Failed to transfer blob at offset", args.Offset, err)
                                return
                            } else {
                                elapsed := time.Since(start_call)
                                blobInfo.mu.Lock()
                                blobInfo.blobsSend++
                                blobInfo.timeElapsed += elapsed
                                blobInfo.mu.Unlock()
                            }
                        }
                    }
                }
            }()
        }

        waitgroup.Wait()
        fmt.Println("Chunks per second:", blobInfo.timeElapsed.Seconds()/float64(blobInfo.blobsSend))
    }
}
