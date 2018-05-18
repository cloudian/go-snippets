package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"io"
	"io/ioutil"
	"math"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Result struct {
	Err          string `json:"err"`
	Bucket       string `json:"bucket"`
	Object       string `json:"object"`
	ObjectSize   string `json:"objectsize"`
	Latency      string `json:"latency"`
	ProcessTime  string `json:"processtime"`
	UploadTime   string `json:"uploadtime"`
	TransferRate string `json:"transferrate"`
	StartTime    int64  `json:"starttime"`
	EndTime      int64  `json:"endtime"`
}

type Job struct {
	Bucket      string `json:"bucket"`
	Keyprefix   string `json:"keyprefix"`
	Objectsize  string `json:"objectsize"`
	osize       int64
	Concurrency int    `json:"concurrency"`
	Partsize    string `json:"partsize"`
	psize       int64
	Maxparts    int    `json:"maxparts"`
	Delparts    bool   `json:"delparts"`
	Workers     int    `json:"workers"`
	Errorlog    string `json:"errorlog"`
	Results     string `json:"results"`
	Count       int    `json:"count"`
}

type ObjectInputStream struct {
	Size      int64
	Pos       int64
	FirstByte bool
	StartTs   time.Time
	CurrentTs time.Time
}

func NewObjectInputStream(size int64) (o *ObjectInputStream) {
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

var region = flag.String("region", "us-east-2", "Region name to be used")
var endpoint = flag.String("endpoint", "", "Overwrite the endpoint")
var profile = flag.String("profile", "default", "The profile name")
var pathstyle = flag.Bool("pathstyle", false, "Enforce path style urls")
var nossl = flag.Bool("nossl", false, "Don't use SSL")
var nomd5 = flag.Bool("nomd5", false, "Disable adding ContentMD5 to S3 object put and uploads")
var nosum = flag.Bool("nosum", false, "Disable creating checksums")
var retries = flag.Int("retries", -1, "Set the number of retries default -1 (forever)")
var cfg = flag.String("config", "objectbench.json", "config file in json format default objectbench.json")
var skeleton = flag.Bool("skeleton", false, "Print a configuration example to stdout")
var help = flag.Bool("h", false, "Print a helpful message.")

func usage() {
	fmt.Println()
	fmt.Println("Use", os.Args[0])
	fmt.Println("\t-region     <region_name> The region name that should be used default us-east-2")
	fmt.Println("\t-endpoint   <endpoint> Set the endpoint to be used default is 'default'")
	fmt.Println("\t            use -nossl if the endpoint is not https")
	fmt.Println("\t-profile    <profile> Set the profile name default is 'default'")
	fmt.Println("\t-pathstyle  If set it enforces path style urls")
	fmt.Println("\t-nossl      Don't use SSL")
	fmt.Println("\t-nomd5      Disable adding ContentMD5 to S3 object put and upload")
	fmt.Println("\t-nosum      Disable creating checksums")
	fmt.Println("\t-retries    Set the number of retries default -1 forever")
	fmt.Println("\t-config     Path to config file")
	fmt.Println("\t-skeleton   Print a configuration file example to stdout and exit")
	fmt.Println()
}

func bytesToUnits(b int64) string {
	if b >= (2 << 39) {
		d := float64(b) / float64(2<<39)
		return fmt.Sprintf("%.2fTB", d)
	}

	if b >= (2 << 29) {
		d := float64(b) / float64(2<<29)
		return fmt.Sprintf("%.2fGB", d)
	}

	if b >= (2 << 19) {
		d := float64(b) / float64(2<<19)
		return fmt.Sprintf("%.2fMB", d)
	}

	if b >= (2 << 9) {
		d := float64(b) / float64(2<<9)
		return fmt.Sprintf("%.2fKB", d)
	}

	return "err"
}

func unitsToBytes(u string) (r int64, err error) {
	if strings.HasSuffix(strings.ToUpper(u), "B") {
		result, err := strconv.ParseInt(strings.TrimSuffix(u, "B"), 10, 64)
		if err == nil {
			return result, nil
		} else {
			return 0, errors.New("Failed to parse number")
		}
	} else if strings.HasSuffix(strings.ToUpper(u), "K") {
		result, err := strconv.ParseInt(strings.TrimSuffix(u, "K"), 10, 64)
		if err == nil {
			return (result * (2 << 9)), nil
		} else {
			return 0, errors.New("Failed to parse number")
		}
	} else if strings.HasSuffix(strings.ToUpper(u), "M") {
		result, err := strconv.ParseInt(strings.TrimSuffix(u, "M"), 10, 64)
		if err == nil {
			return (result * (2 << 19)), nil
		} else {
			return 0, errors.New("Failed to parse number")
		}
	} else if strings.HasSuffix(strings.ToUpper(u), "G") {
		result, err := strconv.ParseInt(strings.TrimSuffix(u, "G"), 10, 64)
		if err == nil {
			return (result * (2 << 29)), nil
		} else {
			return 0, errors.New("Failed to parse number")
		}
	} else if strings.HasSuffix(strings.ToUpper(u), "T") {
		result, err := strconv.ParseInt(strings.TrimSuffix(u, "T"), 10, 64)
		if err == nil {
			return (result * (2 << 39)), nil
		} else {
			return 0, errors.New("Failed to parse number")
		}
	}

	return 0, nil
}

func startJob(session *session.Session, job *Job) {
	defer gwg.Done()
	var wg sync.WaitGroup
	var cv *sync.Cond
	var mu sync.Mutex
	var ready bool = false
	rchan := make(chan Result)

	go func() {
		for {
			result := <-rchan
			//write result
		}
	}()

	for i := 0; i < job.Workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			mu.Lock()
			for ready != true {
				cv.Wait()
			}
			mu.Unlock()

			//Use an AtomixInt to count objects
			//Create a Result that you hand over via a channel to write it
			t := time.Now()
			o := NewObjectInputStream(job.osize)
			bucket := job.Bucket
			filename := fmt.Sprintf("%s%d", job.Keyprefix, count)
			// http://docs.aws.amazon.com/sdk-for-go/api/service/s3/s3manager/#NewUploader
			uploader := s3manager.NewUploader(session, func(u *s3manager.Uploader) {
				u.Concurrency = job.Concurrency
				u.LeavePartsOnError = job.Delparts
				if job.Maxparts > 0 {
					u.MaxUploadParts = job.Maxparts
					u.PartSize = job.psize
				} else {
					u.PartSize = job.psize
				}
			})

			// Upload the file's body to S3 bucket as an object with the key being the
			// same as the filename.
			_, err := uploader.Upload(&s3manager.UploadInput{
				Bucket: aws.String(bucket),
				Key:    aws.String(filename),

				// The file to be uploaded. io.ReadSeeker is preferred as the Uploader
				// will be able to optimize memory when uploading large content. io.Reader
				// is supported, but will require buffering of the reader's bytes for
				// each part.
				Body: o,
			})

			if err != nil {
				rchan <- Result{Err: fmt.Sprintf("Unable to upload %q to %q, %v", filename, bucket, err)}
			} else {
				ptime := (o.CurrentTs.Sub(o.StartTs).Seconds())
				utime := (time.Now().Sub(o.StartTs).Seconds())
				rate := int64((float64(o.Pos)) / utime)
				latency := o.StartTs.Sub(t).Seconds() * 1000
				r := Result{
					Err:          "",
					Bucket:       job.Bucket,
					Object:       filename,
					ObjectSize:   bytesToUnits(o.Size),
					Latency:      fmt.Sprintf("%vms", o.StartTs.Sub(t).Seconds()*1000),
					ProcessTime:  fmt.Sprintf("%vs", ptime),
					UploadTime:   fmt.Sprintf("%vs", utime),
					TransferRate: fmt.Sprintf("%s/s", bytesToUnits(rate)),
					StartTime:    t.UnixNano(),
					EndTime:      time.Now().UnixNano(),
				}
				rchan <- r
			}
		}()
	}

	mu.Lock()
	ready = true
	mu.Unlock()
	cv.Broadcast()
	wg.Wait()
	fmt.Printf("Start session: %v %v\n", session, job)
}

var gwg sync.WaitGroup

func main() {
	flag.Parse()
	if *help {
		usage()
		return
	}

	rawjson, err := ioutil.ReadFile(*cfg)
	if err != nil {
		exitErrorf("Error reading config %v", err)
	}

	var jobs []Job
	err = json.Unmarshal(rawjson, &jobs)
	if err != nil {
		exitErrorf("Error parsing json %v", err)
	}

	for _, j := range jobs {
		j.osize, err = unitsToBytes(j.Objectsize)
		if err != nil {
			exitErrorf("Error parsing json %v", err)
		}

		j.psize, err = unitsToBytes(j.Partsize)
		if err != nil {
			exitErrorf("Error parsing json %v", err)
		}

		if j.Maxparts > 0 {
			j.psize = int64(math.Ceil(float64(j.osize) / float64(j.Maxparts)))
		}

		fmt.Println("Job ", j.Bucket, j.Keyprefix, j.Objectsize, j.osize, j.psize)
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

	sess, err := session.NewSession(config)
	if err != nil {
		exitErrorf("Unable to create session %v", err)
	}

	for _, j := range jobs {
		gwg.Add(1)
		go startJob(sess, &j)
	}

	gwg.Wait()
}

func exitErrorf(msg string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, msg+"\n", args...)
	os.Exit(1)
}
