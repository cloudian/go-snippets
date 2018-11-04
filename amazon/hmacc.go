package main

import (
	"crypto/hmac"
	"crypto/sha256"
	"fmt"
    "os"
)

func main() {
	sk := "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
	sts := "AWS4-HMAC-SHA256\n20130524T000000Z\n20130524/us-east-1/s3/aws4_request\n7344ae5b7ee6c3e7e6b0fe0640412a37625d1fbfff95c48bbb2dc43964946972"
    date := hmac.New(sha256.New, []byte("AWS4"+sk))
    date.Write([]byte("20130524"))
    region := hmac.New(sha256.New, []byte(date.Sum(nil)))
    region.Write([]byte("us-east-1"))
    service := hmac.New(sha256.New, []byte(region.Sum(nil)))
    service.Write([]byte("s3"))
    req := hmac.New(sha256.New, []byte(service.Sum(nil)))
    req.Write([]byte("aws4_request"))
    signature := hmac.New(sha256.New, []byte(req.Sum(nil)))
    signature.Write([]byte(sts))
	fmt.Fprintf(os.Stdout, "%x\n", signature.Sum(nil))
}
