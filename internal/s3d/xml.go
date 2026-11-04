package s3d

import (
	"encoding/xml"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

const s3NS = "http://s3.amazonaws.com/doc/2006-03-01/"

// s3Error is an S3 error answer.
type s3Error struct {
	Code    string
	Message string
	Status  int
}

func (e *s3Error) Error() string { return e.Code + ": " + e.Message }

func errorf(status int, code, format string, args ...any) *s3Error {
	msg := format
	if len(args) > 0 {
		msg = sprintf(format, args...)
	}
	return &s3Error{Code: code, Message: msg, Status: status}
}

var (
	errNoSuchKey    = &s3Error{"NoSuchKey", "The specified key does not exist.", http.StatusNotFound}
	errNoSuchBucket = &s3Error{"NoSuchBucket", "The specified bucket does not exist.", http.StatusNotFound}
	errNoSuchUpload = &s3Error{"NoSuchUpload", "The specified multipart upload does not exist.", http.StatusNotFound}
	errAccessDenied = &s3Error{"AccessDenied", "Access Denied", http.StatusForbidden}
	errSlowDown     = &s3Error{"SlowDown", "Please reduce your request rate.", http.StatusServiceUnavailable}
	// Each SlowDown says which bound refused the request, so an operator
	// reading a client's error, or bstore_s3d_slowdown_total, knows which
	// limit to look at.
	errSlowKeyRate    = &s3Error{"SlowDown", "This access key is over its request rate (limits.requests_per_sec); retry shortly.", http.StatusServiceUnavailable}
	errSlowReplay     = &s3Error{"SlowDown", "This access key signed more writes in the last 15 minutes than the gateway keeps against replay (limits.replay_per_key); retry shortly.", http.StatusServiceUnavailable}
	errSlowWrites     = &s3Error{"SlowDown", "The gateway has its most writes in progress (limits.writes); retry shortly.", http.StatusServiceUnavailable}
	errSlowParts      = &s3Error{"SlowDown", "The gateway has its most part uploads in progress (limits.part_writes); retry shortly.", http.StatusServiceUnavailable}
	errSlowReads      = &s3Error{"SlowDown", "The gateway has its most object reads in progress (limits.reads); retry shortly.", http.StatusServiceUnavailable}
	errSlowStages     = &s3Error{"SlowDown", "The gateway has its most bodies streaming in (limits.stages); retry shortly.", http.StatusServiceUnavailable}
	errSlowLookups    = &s3Error{"SlowDown", "The gateway has its most lookups in progress (limits.lookups); retry shortly.", http.StatusServiceUnavailable}
	errInternal       = &s3Error{"InternalError", "We encountered an internal error. Please try again.", http.StatusInternalServerError}
	errConditional    = &s3Error{"NotImplemented", "Conditional writes (If-Match, If-None-Match) are not supported by this gateway; it holds no lock.", http.StatusNotImplemented}
	errMethod         = &s3Error{"MethodNotAllowed", "The specified method is not allowed against this resource.", http.StatusMethodNotAllowed}
	errMissingLength  = &s3Error{"MissingContentLength", "You must provide the Content-Length HTTP header.", http.StatusLengthRequired}
	errTooLarge       = &s3Error{"EntityTooLarge", "Your proposed upload exceeds the maximum allowed object size.", http.StatusBadRequest}
	errIncomplete     = &s3Error{"IncompleteBody", "You did not provide the number of bytes specified by the Content-Length HTTP header.", http.StatusBadRequest}
	errBadDigest      = &s3Error{"BadDigest", "The Content-MD5 you specified did not match what we received.", http.StatusBadRequest}
	errInvalidDigest  = &s3Error{"InvalidDigest", "The Content-MD5 you specified is not valid.", http.StatusBadRequest}
	errSHA256Mismatch = &s3Error{"XAmzContentSHA256Mismatch", "The provided 'x-amz-content-sha256' header does not match what was computed.", http.StatusBadRequest}
	errMalformedXML   = &s3Error{"MalformedXML", "The XML you provided was not well-formed or did not validate against our published schema.", http.StatusBadRequest}
)

func notImplemented(what string) *s3Error {
	return &s3Error{"NotImplemented", what + " is not implemented by this gateway.", http.StatusNotImplemented}
}

func invalidArgument(msg string) *s3Error {
	return &s3Error{"InvalidArgument", msg, http.StatusBadRequest}
}

type xmlError struct {
	XMLName   xml.Name `xml:"Error"`
	Code      string   `xml:"Code"`
	Message   string   `xml:"Message"`
	Resource  string   `xml:"Resource,omitempty"`
	RequestID string   `xml:"RequestId"`
}

// writeError answers an S3 error; a HEAD carries no body.
func writeError(w http.ResponseWriter, r *http.Request, e *s3Error, reqID string) {
	w.Header().Del("ETag")
	w.Header().Del("Content-Length")
	if r.Method == http.MethodHead {
		w.WriteHeader(e.Status)
		return
	}
	body, _ := xml.Marshal(xmlError{Code: e.Code, Message: e.Message, Resource: r.URL.Path, RequestID: reqID})
	w.Header().Set("Content-Type", "application/xml")
	w.Header().Set("Content-Length", strconv.Itoa(len(xml.Header)+len(body)))
	w.WriteHeader(e.Status)
	_, _ = w.Write([]byte(xml.Header))
	_, _ = w.Write(body)
}

func writeXML(w http.ResponseWriter, status int, v any) {
	body, err := xml.Marshal(v)
	if err != nil {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/xml")
	w.Header().Set("Content-Length", strconv.Itoa(len(xml.Header)+len(body)))
	w.WriteHeader(status)
	_, _ = w.Write([]byte(xml.Header))
	_, _ = w.Write(body)
}

type listAllMyBuckets struct {
	XMLName xml.Name    `xml:"ListAllMyBucketsResult"`
	NS      string      `xml:"xmlns,attr"`
	Owner   owner       `xml:"Owner"`
	Buckets []bucketXML `xml:"Buckets>Bucket"`
}

type owner struct {
	ID          string `xml:"ID"`
	DisplayName string `xml:"DisplayName"`
}

type bucketXML struct {
	Name         string `xml:"Name"`
	CreationDate string `xml:"CreationDate"`
}

type locationXML struct {
	XMLName xml.Name `xml:"LocationConstraint"`
	NS      string   `xml:"xmlns,attr"`
	Value   string   `xml:",chardata"`
}

type listV2 struct {
	XMLName               xml.Name       `xml:"ListBucketResult"`
	NS                    string         `xml:"xmlns,attr"`
	Name                  string         `xml:"Name"`
	Prefix                string         `xml:"Prefix"`
	Marker                *string        `xml:"Marker,omitempty"`
	NextMarker            string         `xml:"NextMarker,omitempty"`
	StartAfter            string         `xml:"StartAfter,omitempty"`
	ContinuationToken     string         `xml:"ContinuationToken,omitempty"`
	NextContinuationToken string         `xml:"NextContinuationToken,omitempty"`
	KeyCount              *int           `xml:"KeyCount,omitempty"`
	MaxKeys               int            `xml:"MaxKeys"`
	Delimiter             string         `xml:"Delimiter,omitempty"`
	EncodingType          string         `xml:"EncodingType,omitempty"`
	IsTruncated           bool           `xml:"IsTruncated"`
	Contents              []objectXML    `xml:"Contents"`
	CommonPrefixes        []commonPrefix `xml:"CommonPrefixes"`
}

type objectXML struct {
	Key               string   `xml:"Key"`
	LastModified      string   `xml:"LastModified"`
	ETag              string   `xml:"ETag"`
	ChecksumAlgorithm []string `xml:"ChecksumAlgorithm,omitempty"`
	ChecksumType      string   `xml:"ChecksumType,omitempty"`
	Size              uint64   `xml:"Size"`
	StorageClass      string   `xml:"StorageClass"`
}

type commonPrefix struct {
	Prefix string `xml:"Prefix"`
}

type initiateMultipart struct {
	XMLName  xml.Name `xml:"InitiateMultipartUploadResult"`
	NS       string   `xml:"xmlns,attr"`
	Bucket   string   `xml:"Bucket"`
	Key      string   `xml:"Key"`
	UploadID string   `xml:"UploadId"`
}

type completeMultipart struct {
	XMLName xml.Name `xml:"CompleteMultipartUpload"`
	Parts   []struct {
		PartNumber int    `xml:"PartNumber"`
		ETag       string `xml:"ETag"`
	} `xml:"Part"`
}

type completeResult struct {
	XMLName        xml.Name `xml:"CompleteMultipartUploadResult"`
	NS             string   `xml:"xmlns,attr"`
	Location       string   `xml:"Location"`
	Bucket         string   `xml:"Bucket"`
	Key            string   `xml:"Key"`
	ETag           string   `xml:"ETag"`
	ChecksumSHA256 string   `xml:"ChecksumSHA256,omitempty"`
	ChecksumType   string   `xml:"ChecksumType,omitempty"`
}

type copyResult struct {
	XMLName        xml.Name `xml:"CopyObjectResult"`
	NS             string   `xml:"xmlns,attr"`
	LastModified   string   `xml:"LastModified"`
	ETag           string   `xml:"ETag"`
	ChecksumSHA256 string   `xml:"ChecksumSHA256,omitempty"`
	ChecksumType   string   `xml:"ChecksumType,omitempty"`
}

type deleteRequest struct {
	XMLName xml.Name `xml:"Delete"`
	Quiet   bool     `xml:"Quiet"`
	Objects []struct {
		Key       string `xml:"Key"`
		VersionID string `xml:"VersionId"`
	} `xml:"Object"`
}

type deleteResult struct {
	XMLName xml.Name      `xml:"DeleteResult"`
	NS      string        `xml:"xmlns,attr"`
	Deleted []deletedXML  `xml:"Deleted"`
	Errors  []deleteError `xml:"Error"`
}

type deletedXML struct {
	Key string `xml:"Key"`
}

type deleteError struct {
	Key     string `xml:"Key"`
	Code    string `xml:"Code"`
	Message string `xml:"Message"`
}

// isoTime is S3's listing time: RFC 3339 in UTC with milliseconds.
func isoTime(ms uint64) string {
	return time.UnixMilli(int64(ms)).UTC().Format("2006-01-02T15:04:05.000Z")
}

// httpTime is an HTTP date.
func httpTime(ms uint64) string { return time.UnixMilli(int64(ms)).UTC().Format(http.TimeFormat) }

// urlEncodeKey is S3's encoding-type=url: each byte that is not unreserved
// or a slash percent-encoded, a space as "+", as S3 has it (clients decode
// with unquote_plus).
func urlEncodeKey(s string) string {
	return strings.ReplaceAll(url.QueryEscape(s), "%2F", "/")
}

// slowReason names the bound behind a SlowDown, for the metric.
func slowReason(e *s3Error) string {
	switch e {
	case errSlowKeyRate:
		return "requests_per_sec"
	case errSlowReplay:
		return "replay_per_key"
	case errSlowWrites:
		return "writes"
	case errSlowParts:
		return "part_writes"
	case errSlowReads:
		return "reads"
	case errSlowStages:
		return "stages"
	case errSlowLookups:
		return "lookups"
	}
	return "other"
}
