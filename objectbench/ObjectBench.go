package main

import (
	"encoding/csv"
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

type Overview struct {
	OpsTotal   int64
	BytesTotal int64
	Start      int64
	Snapshot   int64
	mu         sync.Mutex
}

var overall Overview

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

func (r *Result) ResultArray() []string {
	return []string{
		r.Err,
		r.Bucket,
		r.Object,
		r.ObjectSize,
		r.Latency,
		r.ProcessTime,
		r.UploadTime,
		r.TransferRate,
		fmt.Sprintf("%v", r.StartTime),
		fmt.Sprintf("%v", r.EndTime),
	}
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
	Count       int64  `json:"count"`
	mu          sync.Mutex
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

	return fmt.Sprintf("err %v", b)
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
	cchan := make(chan bool)
	cv = sync.NewCond(&mu)

	go func() {
		f, err := os.OpenFile(job.Results, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			fmt.Printf("Error writing %s: %v\n", job.Results, err)
		}
		defer f.Close()
		w := csv.NewWriter(f)
		fmt.Println("Started result writer.")
		for {
			select {
			case <-cchan:
				break
			case result := <-rchan:
				if err := w.Write(result.ResultArray()); err != nil {
					fmt.Printf("Error writing %s: %v\n", job.Results, err)
				}

				w.Flush()
				if w.Error() != nil {
					fmt.Printf("Error writing %s: %v\n", job.Results, w.Error())
				}
			}
		}
	}()

	for i := 0; i < job.Workers; i++ {
		wg.Add(1)
		go func(nr int) {
			defer wg.Done()
			mu.Lock()
			for ready != true {
				cv.Wait()
			}
			mu.Unlock()

			for {
				job.mu.Lock()
				current := job.Count
				job.Count--
				job.mu.Unlock()
				if current > 0 {
					t := time.Now()
					o := NewObjectInputStream(job.osize)
					bucket := job.Bucket
					filename := fmt.Sprintf("%s%d", job.Keyprefix, current)
					// http://docs.aws.amazon.com/sdk-for-go/api/service/s3/s3manager/#NewUploader
					uploader := s3manager.NewUploader(session, func(u *s3manager.Uploader) {
						u.Concurrency = job.Concurrency
						u.LeavePartsOnError = job.Delparts
						if job.Maxparts > 0 {
							u.MaxUploadParts = job.Maxparts
							u.PartSize = job.psize
						} else {
							if job.psize > 0 {
								u.PartSize = job.psize
							}
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
						overall.mu.Lock()
						overall.BytesTotal += o.Size
						overall.OpsTotal++
						overall.mu.Unlock()
						ptime := (o.CurrentTs.Sub(o.StartTs).Seconds())
						utime := (time.Now().Sub(o.StartTs).Seconds())
						rate := int64((float64(o.Pos)) / utime)
						latency := o.StartTs.Sub(t).Seconds() * 1000
						r := Result{
							Err:          "ok",
							Bucket:       job.Bucket,
							Object:       filename,
							ObjectSize:   bytesToUnits(o.Size),
							Latency:      fmt.Sprintf("%vms", latency),
							ProcessTime:  fmt.Sprintf("%vs", ptime),
							UploadTime:   fmt.Sprintf("%vs", utime),
							TransferRate: fmt.Sprintf("%s/s", bytesToUnits(rate)),
							StartTime:    t.UnixNano(),
							EndTime:      time.Now().UnixNano(),
						}
						rchan <- r
					}
				} else {
					break
				}
			}
		}(i)
	}

	mu.Lock()
	ready = true
	mu.Unlock()
	cv.Broadcast()
	wg.Wait()
	cchan <- true
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

	for j, _ := range jobs {
		if jobs[j].Workers == 0 {
			jobs[j].Workers = 1
		}

		jobs[j].osize, err = unitsToBytes(jobs[j].Objectsize)
		if err != nil {
			exitErrorf("Error parsing json %v", err)
		}
		fmt.Println("Objectsize", jobs[j].Objectsize, "osize", jobs[j].osize)

		jobs[j].psize, err = unitsToBytes(jobs[j].Partsize)
		if err != nil {
			exitErrorf("Error parsing json %v", err)
		}

		if jobs[j].psize != 0 {
			pb, _ := unitsToBytes("5M")
			if jobs[j].psize < pb {
				jobs[j].psize, _ = unitsToBytes("5M")
				fmt.Println("Ignoring part size, min 5M will be used now")
			}
		}

		if jobs[j].Maxparts > 0 {
			jobs[j].psize = int64(math.Ceil(float64(jobs[j].osize) / float64(jobs[j].Maxparts)))
			pb, _ := unitsToBytes("5M")
			if jobs[j].psize < pb {
				jobs[j].psize = 0
				jobs[j].Maxparts = 0
				fmt.Println("Ignoring the number of parts because part size is < 5M")
			}
		}

		fmt.Println("Job ", jobs[j].Bucket, jobs[j].Keyprefix, jobs[j].Objectsize, jobs[j].osize, jobs[j].psize)
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

	var cchan = make(chan bool)
	overall.Start = time.Now().UnixNano()
	go func() {
		for {
			select {
			case <-cchan:
				break
			case <-time.After(1 * time.Second):
				printit := false
				overall.mu.Lock()
				printit = overall.OpsTotal > 0
				obytes := overall.BytesTotal
				oops := overall.OpsTotal
				overall.Snapshot = time.Now().UnixNano()
				seconds := float64(overall.Snapshot-overall.Start) / float64(1000000000)
				bytes := bytesToUnits(int64(float64(overall.BytesTotal) / seconds))
				ops := float64(overall.OpsTotal) / seconds
				overall.mu.Unlock()
				if printit {
					fmt.Printf("%d,%d,%.2f,%.2f,%s/s\n", oops, obytes, seconds, ops, bytes)
				}
			}
		}
	}()

	for _, j := range jobs {
		gwg.Add(1)
		go startJob(sess, &j)
	}

	gwg.Wait()
	cchan <- true
}

func exitErrorf(msg string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, msg+"\n", args...)
	os.Exit(1)
}
