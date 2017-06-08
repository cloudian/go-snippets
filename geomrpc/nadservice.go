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
        "sync"
		"fmt"
		"syscall"
		"os"
		"unsafe"
		"flag"
        "strings"
        "time"
        "net/rpc"
)

// #include <errno.h>
// #include <stdlib.h>
// #include <string.h>
// #include <fcntl.h>
// #include <sys/types.h>
// #include <sys/bio.h>
// #include <geom/gate/g_gate.h>
/*
   int is_null(void *arg) {
    if(arg == NULL) {
        return(1);
    } else {
        return(0);
    }
   }
*/
import "C"

type Disk struct {
    Mediasize int64
    Blocksize int64
    Host string
    Client *rpc.Client
}

type Info struct {
    Mediasize int64
}

type Args struct {
	Offset int64
	Blob []byte
}


var gctl *os.File
var command_flag = flag.String("c", "", "create, destroy, discover, list, attach")
var unit_flag = flag.Int("u", -1, "unit number")
var host_flag = flag.String("h", "", "remote rpc hosts serving this disk separated by comma if more than one, e.g. 1.2.3.4:5001")
var block_flag = flag.Int64("b", 4096, "block size in bytes, default 4096")
var client *rpc.Client
var disks []Disk
var waitgroup sync.WaitGroup

func serve(unit int) {
    defer waitgroup.Done()
    cio := C.struct_g_gate_ctl_io{
            gctl_version: C.G_GATE_VERSION,
            gctl_unit: C.int(unit),
            gctl_error: 0,
            gctl_length: C.off_t(*block_flag),
    }

    var bsize C.off_t = cio.gctl_length
    cio.gctl_data = C.malloc(C.size_t(*block_flag))
    if C.is_null(cio.gctl_data) == 1 {
        panic(fmt.Sprintf("Out of memory for buffer size %d", 
                    C.int(cio.gctl_length)))
    }

    var err C.int = 0
    var arg Args

    for {
L1:
        if bsize > cio.gctl_length {
            cio.gctl_length = bsize
        }
        cio.gctl_error = 0
        _, _, errno := syscall.Syscall(syscall.SYS_IOCTL,
                uintptr(gctl.Fd()),C.G_GATE_CMD_START,
                uintptr(unsafe.Pointer(&cio)))
        if errno != 0 {
            panic("ioctl syscall failed G_GATE_CMD_START")
        }
        err = cio.gctl_error
        switch err {
            case 0:
                break
            case C.ECANCELED:
                fmt.Println("io cancelled")
                break
            case C.ENOMEM:
                if cio.gctl_length > bsize {
                    // Try to get a large enough buffer
                    // no need to shrink it
                    // we get the actual data size in gctl_length
                    fmt.Println("Realloc ENOMEM", cio.gctl_length, bsize)
                    cio.gctl_data = C.realloc(cio.gctl_data, 
                            C.size_t(cio.gctl_length))
                    if C.is_null(cio.gctl_data) == 1 {
                        panic(fmt.Sprintf("Out of memory for buffer size %d", 
                                    C.int(cio.gctl_length)))
                    }
                    bsize = cio.gctl_length
                }
                goto L1
            default:
                fmt.Println("Error", err)

        }

        err = 0
        switch cio.gctl_cmd {
            case C.BIO_READ:
                if cio.gctl_length > bsize {
                    fmt.Println("Realloc ", bsize)
                    fmt.Println("Realloc READ ", cio.gctl_length, bsize)
                    cio.gctl_data = C.realloc(cio.gctl_data, 
                            C.size_t(cio.gctl_length))
                    if C.is_null(cio.gctl_data) == 1 {
                        panic(fmt.Sprintf("Out of memory for buffer size %d", 
                                    C.int(cio.gctl_length)))
                    }
                    bsize = cio.gctl_length
                    //this should not happen
                }

                arg = Args{
                            //Blob: C.GoBytes(unsafe.Pointer(cio.gctl_data),
                            //C.int(cio.gctl_length)),
                            Blob: make([]byte, int64(cio.gctl_length)),
                            Offset: int64(cio.gctl_offset),
                      }

                rch := make(chan *Args)
                for _, disk := range disks {
                    go func (disk *Disk) {
                        iarg := <-rch
                        oarg := new(Args)
                        if ioerr := disk.Client.Call("NadServer.Get", 
                                iarg, oarg); ioerr != nil {
                            fmt.Println("Error reading from", disk.Host, ioerr)
                        } else {
                            rch <- oarg
                        }
                    }(&disk)
                    rch <- &arg
                }

                select {
                    case oarg := <- rch:
                        C.free(cio.gctl_data)
                        bsize = C.off_t(len(oarg.Blob))
                        cio.gctl_length = bsize
                        //C.CBytes allocates memory using C.malloc
                        cio.gctl_data = C.CBytes(oarg.Blob)
                        if C.is_null(cio.gctl_data) == 1 {
                            panic(fmt.Sprintf("Out of memory for buffer size %d", 
                                        C.int(cio.gctl_length)))
                        }
                    case <- time.After(time.Second * 1):
                        fmt.Println("Read timeout for all disks")
                        cio.gctl_error = C.EIO
                }
                break
            case C.BIO_DELETE:
                break
            case C.BIO_WRITE:
                arg = Args{
                        Blob: C.GoBytes(unsafe.Pointer(cio.gctl_data), C.int(cio.gctl_length)), 
                        Offset: int64(cio.gctl_offset),
                      }

                wch := make(chan *Args)
                wrcv := make(chan int, len(disks))
                for _, disk := range disks {
                    go func (disk *Disk) {
                        var reply int = 0
                        iarg := <-wch
                        if ioerr := disk.Client.Call("NadServer.Put", 
                                iarg, &reply); ioerr != nil {
                            fmt.Println("Error writing to", disk.Host, ioerr)
                        } else {
                            wrcv <-reply 
                        }
                    }(&disk)
                    wch <- &arg
                }


                writtenTo := 0
                for {
                    select {
                        case <- wrcv:
                            writtenTo += 1
                            if writtenTo == len(disks) {
                                break
                            }
                        case <- time.After(time.Second * 1):
                            fmt.Println("Write timeout for", 
                                    len(disks)-writtenTo, "disks.")
                            if writtenTo == 0 {
                                cio.gctl_error = C.EIO
                            }
                    }
                    break
                }
                break
            default:
                err = C.EOPNOTSUPP
        }

        cio.gctl_error = err
        _, _, errno = syscall.Syscall(syscall.SYS_IOCTL, 
                                      uintptr(gctl.Fd()),C.G_GATE_CMD_DONE, 
                                      uintptr(unsafe.Pointer(&cio))) 

        if errno != 0 {
            panic("ioctl syscall failed G_GATE_CMD_DONE")
        }
    }

    C.free(cio.gctl_data)
    fmt.Println("serve routine done")
}

func main() {
    flag.Parse()

    if *command_flag == "attach" && (*host_flag == "" || *unit_flag == -1) {
        fmt.Println("attach requires -h and -u")
        return
    }

    if *command_flag == "create" && (*host_flag == "" || *unit_flag == -1) {
        fmt.Println("create requires -h and -u")
        return
    }

    if *command_flag == "destroy" && *unit_flag == -1 {
        fmt.Println("destroy requires a valid unit -u")
        return
    }

    if *command_flag == "discover" && *host_flag == "" {
        fmt.Println("discover requires a comma separeted list of host:port -h")
        return
    }

    if *command_flag == "list" {
        fmt.Println("Use 'geom gate list'")
        return
    }

    if *command_flag == "cancel" && *unit_flag != -1 {
        gctl, err := os.OpenFile("/dev/ggctl", os.O_RDWR, 0644)
        if err != nil {
            fmt.Println(err)
            return
        }
        defer gctl.Close()
        cs := C.struct_g_gate_ctl_cancel{
            gctl_version: C.G_GATE_VERSION, 
            gctl_unit: C.int(*unit_flag),
        }
        _, _, errno := syscall.Syscall(syscall.SYS_IOCTL, 
                                       uintptr(gctl.Fd()),C.G_GATE_CMD_CANCEL, 
                                       uintptr(unsafe.Pointer(&cs))) 
        fmt.Println("cancel", errno)
        return
    }

    var mediasize int64 = 0
    if *host_flag != "" {
        hosts := strings.Split(*host_flag, ",")
        for _, host := range hosts {
            var err error
            client, err = rpc.Dial("tcp", host)
            if err != nil {
                fmt.Println("Unable to connect to rpc server", err)
                return
            } else {
                var info = Info{}
                var oinfo = new(Info)
                if err = client.Call("NadServer.Info", 
                        &info, oinfo); err != nil {
                    fmt.Println("An error occured", err)
                    client.Close()
                    return
                } else {
                    if mediasize == 0 {
                        mediasize = oinfo.Mediasize
                    } else {
                        if mediasize != oinfo.Mediasize {
                            fmt.Println("The backend disks must have the same mediasize.")
                            return
                        }

                    }

                    disks = append(disks, Disk{
                                                Mediasize: mediasize, 
                                                Blocksize: *block_flag, 
                                                Host: host, 
                                                Client: client,
                                              })
                }
            }
        }
    }

    var err error
    gctl, err = os.OpenFile("/dev/ggctl", os.O_RDWR, 0644)
    if err != nil {
        fmt.Println(err)
        return
    }
    defer gctl.Close()

    switch *command_flag {
        case "attach":
            if mediasize == 0 || *block_flag == 0 || 
                (mediasize % *block_flag) != 0 {
                fmt.Println("media size and block size have to be greater than 0")
                fmt.Println("media size has to be a multiple of block size")
                return
            }

            waitgroup.Add(1)
            go serve(*unit_flag)
        case "create":
            if mediasize == 0 || *block_flag == 0 || 
                (mediasize % *block_flag) != 0 {
                fmt.Println("media size and block size have to be greater than 0")
                fmt.Println("media size has to be a multiple of block size")
                return
            }

            cs := C.struct_g_gate_ctl_create{
                gctl_version: C.G_GATE_VERSION,
                gctl_mediasize: C.off_t(mediasize),
                gctl_sectorsize: C.u_int(*block_flag),
                gctl_flags: 0,
                gctl_maxcount: 256,
                gctl_timeout: 1,
                gctl_unit: C.int(*unit_flag),
            }

            C.strcpy(&cs.gctl_info[0], C.CString(*host_flag))
            _, _, errno := syscall.Syscall(syscall.SYS_IOCTL,
                                           uintptr(gctl.Fd()),
                                           C.G_GATE_CMD_CREATE,
                                           uintptr(unsafe.Pointer(&cs)))
            if errno != 0 {
                fmt.Println("create failed", errno)
            }
            waitgroup.Add(1)
            go serve(*unit_flag)
        case "destroy":
            cs := C.struct_g_gate_ctl_destroy{
                gctl_version: C.G_GATE_VERSION,
                gctl_force: 0,
                gctl_unit: C.int(*unit_flag),
            }
            _, _, errno := syscall.Syscall(syscall.SYS_IOCTL,
                                           uintptr(gctl.Fd()),
                                           C.G_GATE_CMD_DESTROY,
                                           uintptr(unsafe.Pointer(&cs)))
            if errno != 0 {
                fmt.Println("destroy failed", errno)
            }
        default:
            fmt.Println("No clue how to handle command '", *command_flag, "'")
    }

    waitgroup.Wait()
}

