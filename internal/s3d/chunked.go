package s3d

import (
	"bufio"
	"crypto/sha256"
	"crypto/subtle"
	"errors"
	"fmt"
	"hash"
	"io"
	"strconv"
	"strings"
)

// The aws-chunked body encoding current AWS SDKs send (spec section 16.1):
// signed chunks (STREAMING-AWS4-HMAC-SHA256-PAYLOAD), each chained to the
// one before from the request's signature; unsigned chunks with trailing
// checksum headers (STREAMING-UNSIGNED-PAYLOAD-TRAILER); and signed chunks
// with trailing headers, themselves signed
// (STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER). The decoder streams: a
// chunk's bytes pass through as they arrive and its signature is checked
// at its end, so a body that fails is refused before anything is
// published, and memory holds one line.

var (
	errChunk     = errors.New("the aws-chunked body is malformed")
	errChunkSig  = errors.New("a chunk signature does not match")
	errTrailer   = errors.New("the trailing headers are malformed")
	errTrailerSg = errors.New("the trailer signature does not match")
)

const (
	maxChunkLine    = 4096
	maxTrailerBytes = 16 << 10
	maxTrailers     = 16
)

type chunkReader struct {
	br      *bufio.Reader
	auth    *Auth // signing context for signed chunks; nil when unsigned
	signed  bool
	trailer bool
	prev    string
	left    int64
	h       hash.Hash
	sig     string
	total   int64
	max     int64 // the decoded length declared; 0 is no bound
	done    bool
	err     error
	// Trailers are the trailing headers, lowercase names, once the body is
	// read to its end.
	Trailers map[string]string
}

func newChunkReader(r io.Reader, a *Auth, signed, trailer bool) *chunkReader {
	return &chunkReader{br: bufio.NewReaderSize(r, 64<<10), auth: a, signed: signed, trailer: trailer, prev: a.Seed,
		h: sha256.New(), Trailers: map[string]string{}}
}

func (c *chunkReader) line() (string, error) {
	var b []byte
	for {
		frag, err := c.br.ReadSlice('\n')
		b = append(b, frag...)
		if len(b) > maxChunkLine {
			return "", errChunk
		}
		if err == bufio.ErrBufferFull {
			continue
		}
		if err == io.EOF {
			return "", io.ErrUnexpectedEOF
		}
		if err != nil {
			return "", err
		}
		break
	}
	if len(b) < 2 || b[len(b)-2] != '\r' {
		return "", errChunk
	}
	return string(b[:len(b)-2]), nil
}

// header reads a chunk header: <hex size>[;chunk-signature=<sig>].
func (c *chunkReader) header() error {
	l, err := c.line()
	if err != nil {
		return err
	}
	size, ext, _ := strings.Cut(l, ";")
	if len(size) == 0 || len(size) > 16 {
		return errChunk
	}
	n, err := strconv.ParseInt(size, 16, 64)
	if err != nil || n < 0 {
		return errChunk
	}
	c.sig = ""
	if c.signed {
		k, v, ok := strings.Cut(ext, "=")
		if !ok || k != "chunk-signature" || len(v) != 64 {
			return errChunk
		}
		c.sig = v
	}
	if c.max > 0 && c.total+n > c.max {
		return errChunk
	}
	c.left = n
	c.h.Reset()
	return nil
}

func (c *chunkReader) endChunk(final bool) error {
	if c.signed {
		want := c.auth.chunkSignature(c.prev, c.h.Sum(nil))
		if subtle.ConstantTimeCompare([]byte(want), []byte(c.sig)) != 1 {
			return errChunkSig
		}
		c.prev = c.sig
	}
	if final {
		return nil
	}
	l, err := c.line()
	if err != nil {
		return err
	}
	if l != "" {
		return errChunk
	}
	return nil
}

func (c *chunkReader) trailers() error {
	var canon strings.Builder
	sig := ""
	for i := 0; ; i++ {
		l, err := c.line()
		if err != nil {
			return err
		}
		if l == "" {
			break
		}
		if i >= maxTrailers || canon.Len()+len(l) > maxTrailerBytes {
			return errTrailer
		}
		k, v, ok := strings.Cut(l, ":")
		if !ok {
			return errTrailer
		}
		k = strings.ToLower(strings.TrimSpace(k))
		v = strings.TrimSpace(v)
		if k == "x-amz-trailer-signature" {
			sig = v
			continue
		}
		c.Trailers[k] = v
		canon.WriteString(k + ":" + v + "\n")
	}
	if c.signed {
		want := c.auth.trailerSignature(c.prev, []byte(canon.String()))
		if subtle.ConstantTimeCompare([]byte(want), []byte(sig)) != 1 {
			return errTrailerSg
		}
	}
	return nil
}

func (c *chunkReader) Read(p []byte) (int, error) {
	if c.err != nil {
		return 0, c.err
	}
	if c.done {
		return 0, io.EOF
	}
	for c.left == 0 {
		if err := c.header(); err != nil {
			c.err = err
			return 0, err
		}
		if c.left == 0 {
			if err := c.endChunk(true); err != nil {
				c.err = err
				return 0, err
			}
			if c.trailer {
				if err := c.trailers(); err != nil {
					c.err = err
					return 0, err
				}
			} else if l, err := c.line(); err != nil || l != "" {
				c.err = errChunk
				return 0, c.err
			}
			c.done = true
			return 0, io.EOF
		}
	}
	if int64(len(p)) > c.left {
		p = p[:c.left]
	}
	n, err := c.br.Read(p)
	c.h.Write(p[:n])
	c.left -= int64(n)
	c.total += int64(n)
	if err == io.EOF {
		c.err = io.ErrUnexpectedEOF
		return n, c.err
	}
	if err != nil {
		c.err = err
		return n, err
	}
	if c.left == 0 {
		if err := c.endChunk(false); err != nil {
			c.err = err
			return n, nil
		}
	}
	return n, nil
}

// decodedLength reads x-amz-decoded-content-length.
func decodedLength(v string) (int64, error) {
	if v == "" {
		return 0, fmt.Errorf("x-amz-decoded-content-length is required with an aws-chunked body")
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil || n < 0 {
		return 0, fmt.Errorf("x-amz-decoded-content-length is not a length")
	}
	return n, nil
}
