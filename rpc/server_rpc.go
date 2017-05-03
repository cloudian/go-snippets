
package main

import (
    "errors"
    "fmt"
    "os"
    "net"
    "net/rpc"
)

type Args struct {
    FileName string
    Offset int64
    FileSize int64
    Blob []byte
}

type NetCopy int

func (t *NetCopy) Put(args *Args, reply *int) error {
    if _, err := os.Stat(args.FileName); os.IsNotExist(err) {
        if fh, err := os.Create(args.FileName); err != nil {
            fmt.Println("Error creating file:", err.Error())
            return err
        } else {
            if err = os.Truncate(args.FileName, args.FileSize); err != nil {
                fmt.Println("Truncate error:", err)
                return err
            } else {
                if written, err := fh.WriteAt(args.Blob, args.Offset); err != nil {
                    fmt.Println("Error writing to file:", err)
                    fh.Close()
                    return err;
                } else {
                    fmt.Println("Written", written)
                    fh.Close()
                    return nil
                }
            }
        }
    } else {
        if fh, err := os.OpenFile(args.FileName, os.O_WRONLY, 0644); err != nil {
            fmt.Println("Unable to open", args.FileName, "for writing", err)
            return errors.New("Unable to open "+ args.FileName + " for writing " + err.Error()) 
        } else {
            if written, err := fh.WriteAt(args.Blob, args.Offset); err != nil {
                fmt.Println("Error writing to file:", err)
                fh.Close()
                return err;
            } else {
                fmt.Println("Written", written)
                *reply = 1
                fh.Close()
                return nil
            }
        }
    }
}

func main() {
    netService := new(NetCopy)
    rpc.Register(netService)
    listener, err := net.Listen("tcp", ":10002")
    if err != nil {
        fmt.Println("Fatal:", err)
        return
    }

    rpc.Accept(listener)
}

