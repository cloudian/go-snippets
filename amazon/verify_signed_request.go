package main

// https://docs.aws.amazon.com/AmazonS3/latest/API/sig-v4-header-based-auth.html
// single chunk
// next multiple chunks
import (
	"crypto/hmac"
	"crypto/sha256"
	"errors"
	"fmt"
	"github.com/dampmann/s3server/auth"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
//	"os"
	"sort"
	"strings"
)

const (
	algo              = "AWS4-HMAC-SHA256"
	timeFormat        = "20060102T150405Z"
	shortTimeFormat   = "20060102"
	emptyStringSHA256 = `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855`
	sk                = `Tg6t292ou0Sz39aRdg9ZvzsWVuoqrxNO5/Ivf+T8`
	ak                = `AKIAJPHMSAV7NE62XELA`
)

type Authorization struct {
	Algorithm     string
	Scope         string
	AccessKey     string
	SignedHeaders []string
	Signature     string
}

func isAnonymous(authorization string) bool {
	if authorization == "" {
		return true
	} else {
		return false
	}
}

func NewAuth(authorization string) (*Authorization, error) {
	fmt.Println("Authorization", authorization)
	if !strings.HasPrefix(authorization, algo) {
		return nil, errors.New(
			fmt.Sprintf("Authorization Header should start with %s", algo))
	}

	authorization = strings.TrimPrefix(authorization, algo)
	authorization = strings.Replace(authorization, " ", "", -1)
	if strings.Index(authorization, "Credential=") == -1 {
		return nil, errors.New("Expected 'Credential=' in Authorization Header")
	}

	if strings.Index(authorization, "SignedHeaders=") == -1 {
		return nil, errors.New("Expected 'SignedHeaders=' in Authorization Header")
	}

	if strings.Index(authorization, "Signature=") == -1 {
		return nil, errors.New("Expected 'Signature=' in Authorization Header")
	}

	a := &Authorization{}
	a.Algorithm = algo
	fields := strings.Split(authorization, ",")

	if len(fields) == 0 {
		return nil, errors.New("Unable to parse authorization header (empty).")
	}

	for i := 0; i < len(fields); i++ {
		if strings.HasPrefix(fields[i], "Credential=") {
			fields[i] = strings.Replace(fields[i], "Credential=", "", -1)
			a.Scope = fields[i]
			creds := strings.Split(a.Scope, "/")
			if len(creds) > 0 {
				a.AccessKey = creds[0]
			} else {
				return nil, errors.New("Unable to parse scope for access key.")
			}
		}

		if strings.HasPrefix(fields[i], "SignedHeaders=") {
			fields[i] = strings.Replace(fields[i], "SignedHeaders=", "", -1)
			a.SignedHeaders = strings.Split(fields[i], ";")
		}

		if strings.HasPrefix(fields[i], "Signature=") {
			fields[i] = strings.Replace(fields[i], "Signature=", "", -1)
			a.Signature = fields[i]
		}
	}

	return a, nil
}

func canonicalQuery(u url.URL) string {
	queryString := ""
	m := u.Query()
	var cm = make(map[string]string)
	for k := range m {
		cm[url.QueryEscape(k)] = url.QueryEscape(m[k][0])
	}

	keys := make([]string, 0, len(cm))
	for k := range cm {
		keys = append(keys, k)
	}

	sort.Strings(keys)
	for index, k := range keys {
		queryString += fmt.Sprintf("%s=%s", k, cm[k])
		if index != len(keys)-1 {
			queryString += "&"
		}
	}

	return queryString
}

func canonicalHeaders(header http.Header, auth *Authorization) string {
	ch := ""
	sort.Strings(auth.SignedHeaders)
	for _, v := range auth.SignedHeaders {
		ch += fmt.Sprintf("%s:%s\n", strings.ToLower(v), strings.TrimSpace(header.Get(v)))
	}

	return ch
}

func canonicalRequest(u url.URL) string {
	var requestURI string
	if len(u.Opaque) > 0 {
		requestURI = "/" + strings.Join(strings.Split(u.Opaque, "/")[3:], "/")
	} else {
		requestURI = u.EscapedPath()
	}

	if len(requestURI) == 0 {
		requestURI = "/"
	}

	return requestURI
}

func getHashedPayload(r *http.Request) (string, []byte) {
	// if the payload is large that will cause trouble
	//handle unsigned payload
	b, err := ioutil.ReadAll(r.Body)
	if err != nil {
		fmt.Println("Error: %v", err)
		return "", nil
	}
	h := sha256.New()
	h.Write(b)
	return fmt.Sprintf("%x", h.Sum(nil)), b
}

func getHashedCanonicalRequest(req string) string {
	h := sha256.New()
	h.Write([]byte(req))
	return fmt.Sprintf("%x", h.Sum(nil))
}

func sign(rregion string, rdate string, stringToSign string) string {
	date := hmac.New(sha256.New, []byte("AWS4"+sk))
	date.Write([]byte(rdate))
	region := hmac.New(sha256.New, []byte(date.Sum(nil)))
	region.Write([]byte(rregion))
	service := hmac.New(sha256.New, []byte(region.Sum(nil)))
	service.Write([]byte("s3"))
	req := hmac.New(sha256.New, []byte(service.Sum(nil)))
	req.Write([]byte("aws4_request"))
	signature := hmac.New(sha256.New, []byte(req.Sum(nil)))
	signature.Write([]byte(stringToSign))
	return fmt.Sprintf("%x", signature.Sum(nil))
}

func RequestHandler(w http.ResponseWriter, r *http.Request) {
	if !isAnonymous(r.Header.Get("Authorization")) {
		err := auth.VerifyRequestSignature(r)
		if err != nil {
			http.Error(w, fmt.Sprintf("%s", err), 403)
		}
		/*
		   		auth, err := NewAuth(r.Header.Get("Authorization"))
		   		if err != nil {
		   			fmt.Println(err)
		               http.Error(w, fmt.Sprintf("%s",err), 400)
		   			return
		   		}

		           //I read the body to generate the hashed payload
		           //and return the body in body since I can't read
		           //it twice (at least I believe so)
		   		body := make([]byte, 0)
		   		hashedPayload := ""
		   		if r.ContentLength == 0 {
		   			hashedPayload = emptyStringSHA256
		   		} else {
		   			hashedPayload, body = getHashedPayload(r)
		   		}

		   		r.Header.Add("Host", r.Host)
		   		canonicalRequest := fmt.Sprintf("%s\n%s\n%s\n%s\n%s\n%s",
		   			r.Method,
		   			canonicalRequest(*r.URL),
		   			canonicalQuery(*r.URL),
		   			canonicalHeaders(r.Header, auth),
		   			strings.Join(auth.SignedHeaders, ";"),
		   			hashedPayload)
		   		fmt.Fprintf(os.Stdout, "\n==\n%v\n==\n", canonicalRequest)
		   		hcr := getHashedCanonicalRequest(canonicalRequest)
		   		fmt.Println(hcr)
		   		scope := strings.Split(auth.Scope, "/")
		   		stringToSign := fmt.Sprintf("%s\n%s\n%s\n%s",
		   			algo,
		   			r.Header.Get("x-amz-date"),
		   			strings.Join(scope[1:], "/"),
		   			hcr)
		   		fmt.Println(stringToSign)
		   		fmt.Println(auth.Scope)
		   		signature := sign(scope[2], scope[1], stringToSign)
		           if signature != auth.Signature {
		               fmt.Println("+++ signature does not match +++")
		               http.Error(w, "SignatureDoesNotMatch", 403)
		               return
		           }
		           fmt.Println("+++ authenticated ", signature, "+++")
		           err = ioutil.WriteFile("thefile", body, 0644)
		           if err != nil {
		               fmt.Println("Error writing body", err)
		           }
		*/

	} else {
		fmt.Println("*** else ***")
	}
	w.Write([]byte(""))
}

func main() {
	http.HandleFunc("/", RequestHandler)
	log.Fatal(http.ListenAndServe(":8080", nil))
}
