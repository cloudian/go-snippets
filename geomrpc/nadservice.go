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

import "fmt"
import "syscall"
import "os"
import "unsafe"
import "flag"
// #include <fcntl.h>
// #include <sys/types.h>
// #include <sys/bio.h>
// #include <geom/gate/g_gate.h>
import "C"

var flag_one = flag.String("c", "", "create or destroy")
var the_unit = flag.Int("u", -1, "unit number")

func serve(unit C.int) {
    cio := C.struct_g_gate_ctl_io{
            gctl_version: C.G_GATE_VERSION,
            gctl_unit: unit,
            gctl_error: 0,
    }

    for {
        // send start
        switch cio.gctl_cmd {
            case C.BIO_READ:

            case C.BIO_DELETE:

            case C.BIO_WRITE:

            default:
        } 
        //send done
    }
}

/*
 struct g_gate_ctl_io {                                                          
     u_int        gctl_version;                                                  
     int      gctl_unit;                                                         
     uintptr_t    gctl_seq;                                                      
     u_int        gctl_cmd;                                                      
     off_t        gctl_offset;                                                   
     off_t        gctl_length;                                                   
     void        *gctl_data;                                                     
     int      gctl_error;                                                        
 };
*/
func main() {
    flag.Parse()

    gctl, err := os.OpenFile("/dev/ggctl", os.O_RDWR, 0644)
    if err != nil {
        fmt.Println(err)
        return
    }
    defer gctl.Close()

    switch *flag_one {
        case "create":
            cs := C.struct_g_gate_ctl_create{
                gctl_version: C.G_GATE_VERSION, 
                gctl_mediasize: 107374182400, 
                gctl_sectorsize: 4096, 
                gctl_flags: 0,
                gctl_maxcount: 16, 
                gctl_timeout: 1, 
                gctl_unit: C.int(*the_unit),
            }
            _, _, errno := syscall.Syscall(syscall.SYS_IOCTL, uintptr(gctl.Fd()),C.G_GATE_CMD_CREATE, uintptr(unsafe.Pointer(&cs))) 
            fmt.Println("create", errno)
        case "destroy":
            cs := C.struct_g_gate_ctl_destroy{
                gctl_version: C.G_GATE_VERSION, 
                gctl_force: 0,
                gctl_unit: C.int(*the_unit),
            }
            _, _, errno := syscall.Syscall(syscall.SYS_IOCTL, uintptr(gctl.Fd()),C.G_GATE_CMD_DESTROY, uintptr(unsafe.Pointer(&cs))) 
            fmt.Println("destroy", errno)
        default:
            fmt.Println("No clue how to handle command '", *flag_one, "'")
    }
}

