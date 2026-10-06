package s3d

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"net/http"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/lightwebinc/bstore/internal/reader"
	"github.com/lightwebinc/bstore/storerec"
)

// Listing (spec section 16.2): `list` pages, each proven complete against
// the latest token (spec section 11.3), merged with the gateway's own
// pending entries; Delimiter applied by the gateway to the proven keys;
// the continuation token the base64 of the last key (or, after a common
// prefix, of the cursor past every key under it); MaxKeys at most 1000.

const maxListPages = 2000

// lister walks the keys of one range in order, proven pages and pending
// entries merged, each key once with the entry that decides it.
type lister struct {
	// list is one proven page of the range after a cursor.
	list   func(after string) (*reader.Page, error)
	prefix string
	cursor string

	buf   []*reader.Entry
	next  string // the next page's cursor
	done  bool   // no page after buf
	pages int

	pend []*pendingEntry
}

func (l *lister) fill() *s3Error {
	for len(l.buf) == 0 && !l.done {
		if l.pages++; l.pages > maxListPages {
			return errSlowDown
		}
		p, err := l.list(max(l.next, l.cursor))
		if err != nil {
			return readError(err)
		}
		for _, e := range p.Entries {
			if e.Entry.Key > l.cursor {
				l.buf = append(l.buf, e)
			}
		}
		l.next, l.done = p.Next, p.Done
	}
	return nil
}

// peek is the next key after the cursor and the entry that decides it;
// ok is false at the end of the range.
func (l *lister) peek() (string, *storerec.Entry, bool, *s3Error) {
	for {
		for len(l.pend) > 0 && l.pend[0].Key <= l.cursor {
			l.pend = l.pend[1:]
		}
		for len(l.buf) > 0 && l.buf[0].Entry.Key <= l.cursor {
			l.buf = l.buf[1:]
		}
		if len(l.buf) == 0 && !l.done {
			if e := l.fill(); e != nil {
				return "", nil, false, e
			}
			continue
		}
		var a *reader.Entry
		var p *pendingEntry
		if len(l.buf) > 0 {
			a = l.buf[0]
		}
		if len(l.pend) > 0 {
			p = l.pend[0]
		}
		switch {
		case a == nil && p == nil:
			return "", nil, false, nil
		case p == nil || a != nil && a.Entry.Key < p.Key:
			return a.Entry.Key, a.Entry, true, nil
		case a == nil || p.Key < a.Entry.Key:
			if p.Kind == storerec.KindPut && p.decoded() != nil {
				return p.Key, p.decoded(), true, nil
			}
			l.cursor = p.Key
		default:
			c := hex.EncodeToString(a.C[:])
			if !newer(p.Created, p.C, a.Entry.Created, c) {
				return a.Entry.Key, a.Entry, true, nil
			}
			if p.Kind == storerec.KindPut && p.decoded() != nil {
				return p.Key, p.decoded(), true, nil
			}
			l.cursor = p.Key
		}
	}
}

type listed struct {
	key   string
	entry *storerec.Entry // nil for a common prefix
}

type listing struct {
	items     []listed
	last      string
	truncated bool
}

// walk takes up to maxKeys keys and common prefixes from the cursor on.
// A key holding delim after the prefix is folded into its common prefix,
// and the walk resumes past every key under it.
func (l *lister) walk(delim string, maxKeys int) (*listing, *s3Error) {
	out := &listing{}
	for maxKeys > 0 {
		key, e, ok, err := l.peek()
		if err != nil {
			return nil, err
		}
		if !ok {
			break
		}
		if len(out.items) == maxKeys {
			out.truncated = true
			break
		}
		if delim != "" {
			if i := strings.Index(key[len(l.prefix):], delim); i >= 0 {
				cp := key[:len(l.prefix)+i+len(delim)]
				out.items = append(out.items, listed{key: cp})
				l.cursor = maxAfter(cp)
				out.last = l.cursor
				continue
			}
		}
		out.items = append(out.items, listed{key: key, entry: e})
		l.cursor = key
		out.last = key
	}
	return out, nil
}

// maxAfter is the greatest object key that starts with p: p padded to the
// key bound with the greatest code points that fit. Every key under p
// sorts at or before it.
func maxAfter(p string) string {
	room := storerec.MaxObjectKey - len(p)
	if room <= 0 {
		return p
	}
	var b strings.Builder
	b.WriteString(p)
	for room >= 4 {
		b.WriteRune(utf8.MaxRune)
		room -= 4
	}
	switch room {
	case 3:
		b.WriteRune(0xFFFF)
	case 2:
		b.WriteRune(0x7FF)
	case 1:
		b.WriteByte('~')
	}
	return b.String()
}

func listParam(v string) bool {
	return len(v) <= storerec.MaxObjectKey && utf8.ValidString(v) && storerec.OneLine(v)
}

// listObjects is ListObjectsV2 (v2) and ListObjects.
func (s *Server) listObjects(ctx context.Context, w http.ResponseWriter, r *http.Request, b *bucket, v2 bool) *s3Error {
	q := r.URL.Query()
	prefix, delim, enc := q.Get("prefix"), q.Get("delimiter"), q.Get("encoding-type")
	if enc != "" && enc != "url" {
		return invalidArgument("Invalid Encoding Method specified in Request")
	}
	if !listParam(prefix) || !utf8.ValidString(delim) || len(delim) > storerec.MaxObjectKey {
		return invalidArgument("the prefix or the delimiter is not a valid key part")
	}
	maxKeys := 1000
	if v := q.Get("max-keys"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 0 {
			return invalidArgument("Provided max-keys not an integer or within integer range")
		}
		maxKeys = min(n, 1000)
	}
	var cursor, token, startAfter, marker string
	if v2 {
		startAfter = q.Get("start-after")
		token = q.Get("continuation-token")
		_, hasToken := q["continuation-token"]
		if hasToken {
			raw, err := base64.StdEncoding.DecodeString(token)
			if err != nil || len(raw) == 0 || !listParam(string(raw)) {
				return invalidArgument("The continuation token provided is incorrect")
			}
			cursor = string(raw)
		} else {
			if !listParam(startAfter) {
				return invalidArgument("start-after is not a valid key")
			}
			cursor = startAfter
		}
	} else {
		marker = q.Get("marker")
		if !listParam(marker) {
			return invalidArgument("marker is not a valid key")
		}
		cursor = marker
	}
	done, e := acquire(s.lookups)
	if e != nil {
		return e
	}
	defer done()
	l := &lister{prefix: prefix, cursor: cursor, next: cursor,
		list: func(after string) (*reader.Page, error) { return s.reader.List(ctx, b.chain, prefix, after) }}
	l.pend = s.st.inRange(b.chain.Bucket, prefix, cursor)
	encode := func(v string) string {
		if enc == "url" {
			return urlEncodeKey(v)
		}
		return v
	}
	res := listV2{NS: s3NS, Name: b.name, Prefix: encode(prefix), MaxKeys: maxKeys, Delimiter: encode(delim), EncodingType: enc}
	got, e := l.walk(delim, maxKeys)
	if e != nil {
		return e
	}
	for _, it := range got.items {
		if it.entry == nil {
			res.CommonPrefixes = append(res.CommonPrefixes, commonPrefix{Prefix: encode(it.key)})
			continue
		}
		e := it.entry
		res.Contents = append(res.Contents, objectXML{Key: encode(it.key), LastModified: isoTime(e.Created), ETag: etagOf(e),
			ChecksumAlgorithm: []string{"SHA256"}, ChecksumType: "FULL_OBJECT", Size: e.Size, StorageClass: "STANDARD"})
	}
	count := len(got.items)
	truncated, last := got.truncated, got.last
	res.IsTruncated = truncated
	if v2 {
		res.KeyCount = &count
		if token != "" {
			res.ContinuationToken = token
		}
		if startAfter != "" {
			res.StartAfter = encode(startAfter)
		}
		if truncated {
			res.NextContinuationToken = base64.StdEncoding.EncodeToString([]byte(last))
		}
	} else {
		res.Marker = &marker
		if marker != "" {
			m := encode(marker)
			res.Marker = &m
		}
		if truncated {
			res.NextMarker = encode(last)
		}
	}
	writeXML(w, http.StatusOK, res)
	return nil
}
