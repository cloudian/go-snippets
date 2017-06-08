/*
MIT License

Copyright (c) 2017 Peer Dampmann

Permission is hereby granted, free of charge, to any person obtaining a copy of
this software and associated documentation files (the "Software"), to deal in 
the Software without restriction, including without limitation the rights to 
use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies 
of the Software, and to permit persons to whom the Software is furnished to do 
so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all 
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR 
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR 
COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER 
IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN 
CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
*/

package main

import (
    "errors"
    "fmt"
    "os"
    "net"
    "net/rpc"
    "flag"
    "syscall"
)

var storage_backend = flag.String("d", "", "file or device to use as storage backend")
var service_port = flag.Int("p", 10000, "service port")
var media_size = flag.Int64("s", 0, "media size in bytes")

type Info struct {
    Mediasize int64
}

var mediaInfo Info = Info{}
var rfh *os.File
var wfh *os.File

type Args struct {
    Offset int64
    Blob []byte
}

type NadServer int

func (t *NadServer) Info(info *Info, oinfo *Info) error {
    fmt.Println("Call Info", mediaInfo.Mediasize)
    oinfo.Mediasize = mediaInfo.Mediasize
    return nil
}

func (t *NadServer) Get(args *Args, rargs *Args) error {
    fmt.Println("Get call", len(args.Blob))
    rargs.Blob = make([]byte, len(args.Blob))
    bytesRead, err := rfh.ReadAt(rargs.Blob, args.Offset)
    if err != nil {
        fmt.Println("Error reading from file:", err)
        return err
    }

    if bytesRead == len(args.Blob) {
        return nil
    } else {
        fmt.Println(fmt.Sprintf("Unable to read %d bytes at offset %d got %d", len(args.Blob), args.Offset, bytesRead))
        return errors.New(fmt.Sprintf("Unable to read %d bytes at offset %d got %d", len(args.Blob), args.Offset, bytesRead))
    }
}

func (t *NadServer) Put(args *Args, reply *int) error {
    fmt.Println("Put call")
    if written, err := wfh.WriteAt(args.Blob, args.Offset); err != nil {
        fmt.Println("Error writing to file:", err)
        return err;
    } else {
        fmt.Println("Written", written)
        *reply = 0
        return nil
    }
}

func main() {
    flag.Parse()
    var err error
    if fileInfo, err := os.Stat(*storage_backend); err != nil {
        fmt.Println("Failed to open", *storage_backend, err)
        return
    } else {
        if fileInfo.Size() == 0 {
            var sb syscall.Stat_t
            if err = syscall.Stat(*storage_backend, &sb); err != nil {
                fmt.Println("Stat failed.", err)
                return
            } else {
                if sb.Size == 0 && *media_size == 0 {
                    fmt.Println("You have to specify the media size using -s in bytes")
                    return
                } else {
                    if sb.Size == 0 {
                        mediaInfo.Mediasize = *media_size
                    } else {
                        mediaInfo.Mediasize = sb.Size
                    }
                    fmt.Println("Mediasize is", mediaInfo.Mediasize)
                }
            }
        } else {
            fmt.Println("Mediasize is", fileInfo.Size())
            mediaInfo.Mediasize = fileInfo.Size()
            fmt.Println("Mediasize is", mediaInfo.Mediasize)
        }
    }


    wfh, err = os.OpenFile(*storage_backend, os.O_WRONLY, 0644)
    if err != nil {
        fmt.Println("Failed top open", *storage_backend, "for writing.", err)
        return
    }

    rfh, err = os.OpenFile(*storage_backend, os.O_RDONLY, 0644)
    if err != nil {
        fmt.Println("Failed top open", *storage_backend, "for reading.", err)
        wfh.Close()
        return
    }

    netService := new(NadServer)
    rpc.Register(netService)
    listener, err := net.Listen("tcp", fmt.Sprintf(":%d", *service_port))
    if err != nil {
        fmt.Println("Fatal:", err)
        return
    }

    rpc.Accept(listener)
}

