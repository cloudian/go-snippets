package main

import (
	"flag"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"io"
	"math/rand"
	"os"
	"time"
)

type ObjectInputStream struct {
	Size      int64
	Pos       int64
	FirstByte bool
	StartTs   time.Time
	CurrentTs time.Time
}

func NewObjectInputStream(size int64, chunkSize int64) (o *ObjectInputStream) {
	return &ObjectInputStream{
		Size:      size,
		Pos:       0,
		FirstByte: true,
	}
}

func (cin *ObjectInputStream) Read(b []byte) (n int, err error) {
	if cin.Pos >= cin.Size {
		return 0, io.EOF
	}

	if cin.FirstByte {
		cin.FirstByte = false
		cin.StartTs = time.Now()
	}

	var sz int64 = 0
	if int64(len(b)) > (cin.Size - cin.Pos) {
		sz = (cin.Size - cin.Pos)
	} else {
		sz = int64(len(b))
	}

	buffer := make([]byte, sz)
	_, _ = rand.Read(buffer)
	copied := copy(b, buffer)

	cin.Pos += int64(copied)
	cin.CurrentTs = time.Now()
	n = copied
	return
}

func (cin *ObjectInputStream) Seek(offset int64, whence int) (int64, error) {
	switch whence {
	case io.SeekStart:
		if offset > cin.Size || (cin.Size+offset) < 0 {
			return 0, io.EOF
		} else {
			cin.Pos = offset
			return offset, nil
		}
	case io.SeekCurrent:
		if (cin.Pos+offset) > cin.Size || (cin.Size+offset) < 0 {
			return 0, io.EOF
		} else {
			cin.Pos += offset
			return offset, nil
		}
	case io.SeekEnd:
		if (cin.Size+offset) > cin.Size || (cin.Size+offset) < 0 {
			return 0, io.EOF
		} else {
			cin.Pos += offset
			return offset, nil
		}
	}

	return 0, nil
}

var bucketname = flag.String("b", "cloudian-auto-spots", "The bucket to store your object")
var objname = flag.String("o", "test/objtest", "The object name (key)")
var objsize = flag.Int64("s", 10485760, "Object size (default 10MB)")
var chunksize = flag.Int64("c", 5*1024*1024, "The size of parts in bytes used min 5MB")
var region = flag.String("r", "us-east-2", "Region name to be used")
var endpoint = flag.String("e", "", "Overwrite the endpoint")
var profile = flag.String("p", "default", "The profile name")
var pathstyle = flag.Bool("P", false, "Enforce path style urls")
var nossl = flag.Bool("nossl", false, "Don't use SSL")
var nomd5 = flag.Bool("nomd5", false, "Disable adding ContentMD5 to S3 object put and uploads")
var nosum = flag.Bool("nosum", false, "Disable creating checksums")
var retries = flag.Int("retries", -1, "Set the number of retries default -1 (forever)")
var maxparts = flag.Int("maxparts", 0, "Will devide the object size by this number to calculate the part size")
var maxthreads = flag.Int("maxthreads", 0, "number of threads to use per object default 0 (sdk default)")
var delparts = flag.Bool("delparts", true, "Delete parts on errors")
var help = flag.Bool("h", false, "Print a helpful message.")

func usage() {
	fmt.Println("Use", os.Args[0])
	fmt.Println("\t-b <bucket_name> default 'cloudian-auto-spots'")
	fmt.Println("\t-o <objectname> default 'objtest'")
	fmt.Println("\t-s <size> object size default 10MB")
	fmt.Println("\t-c <chunk_size> The size of parts used in bytes min 5MB")
	fmt.Println("\t-r <region_name> The region name that should be used")
	fmt.Println("\t-e <endpoint> Set the endpoint to be used default is 'default'")
	fmt.Println("\t-p <profile> Set the profile name default is 'default'")
	fmt.Println("\t-P If set it enforces path style urls")
	fmt.Println("\t-nossl Don't use SSL")
	fmt.Println("\t-nomd5 Disable adding ContentMD5 to S3 object put and upload")
	fmt.Println("\t-nosum Disable creating checksums")
	fmt.Println("\t-retries Set the number of retries default -1 forever")
	fmt.Println("\t-maxparts <count> Will devide the object size by this number to calculate the part size")
	fmt.Println("\t-maxthreads <count> number of threads to use per object default 0 (sdk default)")
	fmt.Println("\t-delparts Delete parts on error, default true")
}

func main() {
	flag.Parse()
	if *help {
		usage()
		return
	}

	config := aws.NewConfig().
		WithCredentials(credentials.NewSharedCredentials("", *profile)).
		WithEndpoint(*endpoint).
		WithRegion(*region).
		WithDisableSSL(*nossl).
		WithMaxRetries(*retries).
		WithDisableComputeChecksums(*nosum).
		WithS3DisableContentMD5Validation(*nomd5).
		WithS3ForcePathStyle(*pathstyle)
	t := time.Now()
	o := NewObjectInputStream(*objsize, *chunksize)
	bucket := *bucketname
	filename := *objname

	sess, err := session.NewSession(config)

	// http://docs.aws.amazon.com/sdk-for-go/api/service/s3/s3manager/#NewUploader
	uploader := s3manager.NewUploader(sess, func(u *s3manager.Uploader) {
		u.Concurrency = *maxthreads
		u.LeavePartsOnError = *delparts
		if *maxparts > 0 && o.Size > u.PartSize {
			u.MaxUploadParts = *maxparts
		}
	})

	// Upload the file's body to S3 bucket as an object with the key being the
	// same as the filename.
	_, err = uploader.Upload(&s3manager.UploadInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(filename),

		// The file to be uploaded. io.ReadSeeker is preferred as the Uploader
		// will be able to optimize memory when uploading large content. io.Reader
		// is supported, but will require buffering of the reader's bytes for
		// each part.
		Body: o,
	})
	if err != nil {
		// Print the error and exit.
		exitErrorf("Unable to upload %q to %q, %v", filename, bucket, err)
	}

	ptime := (o.CurrentTs.Sub(o.StartTs).Seconds())
	utime := (time.Now().Sub(o.CurrentTs).Seconds())
	rate := (o.CurrentTs.Sub(o.StartTs).Seconds())
	latency := o.StartTs.Sub(t).Seconds() * 1000
	fmt.Println("#bucketname,objectname,objectsize in bytes,latency in ms,ptime in s,uploadtime in s,transferrate MB/s")
	fmt.Printf("%s,%s,%v,%v,%v,%v,%v\n", bucket, filename, *objsize, latency, ptime, utime, rate)
	//fmt.Printf("Successfully uploaded %q (%v Bytes) to %q\n", filename, *objsize, bucket)
	//fmt.Println("Latency ", o.StartTs.Sub(t).Seconds()*1000, "ms")
	//fmt.Println("Transfer to aws sdk took ", (o.CurrentTs.Sub(o.StartTs).Seconds()), " s")
	//fmt.Println("Upload took ", (time.Now().Sub(o.CurrentTs).Seconds()), "s")
	//fmt.Println("Transfer rate ", ((float64(o.Pos) / (1024 * 1024)) / (time.Now().Sub(o.CurrentTs).Seconds())), " mega bytes/s")
}

func exitErrorf(msg string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, msg+"\n", args...)
	os.Exit(1)
}
