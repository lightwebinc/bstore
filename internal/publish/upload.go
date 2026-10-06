package publish

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/lightwebinc/bcommon/chirp"
)

// Unicast pieces through BRC-167's baseline upload sessions (spec section
// 14): with no facade, a closure goes to each provider on its own, over
// BRC-104, at <public>/chirp/v1/uploads: a session opened, every object
// staged (an object the session already holds is skipped), and the root
// committed. A provider answers the commit 202 until an entry naming the
// root reaches it and it takes it, and 201 with the advertised location
// after (frozen.md section 14, row C9); the entry goes to the provider's
// submit route as it does to a host. A provider that is not reached is
// owed the closure: the debt is recorded before the first upload and
// retried (RetryUploads) until it is paid or a week has passed.

// UploadRecord is a closure owed to a provider's upload sessions.
type UploadRecord struct {
	Root     string `json:"root"`
	Provider string `json:"provider"`
	// Retention is the retention asked, seconds.
	Retention uint64 `json:"retention"`
	Created   int64  `json:"created"`
	Tries     int    `json:"tries,omitempty"`
	Last      string `json:"last,omitempty"`
}

// UploadResult is one provider's answer to a commit.
type UploadResult struct {
	Provider string
	// Status is 201 (advertised) or 202 (staged until an entry arrives).
	Status int
	Staged int
	Err    error
}

// uploadClosure uploads a closure to every provider of uploads, recording
// each as owed before it starts and clearing it once the commit is taken.
// It returns each provider's result; the put proceeds when at least one
// took the closure.
func (p *Publisher) uploadClosure(ctx context.Context, root [32]byte, c Closure, retention uint64) []UploadResult {
	rootHex := hex.EncodeToString(root[:])
	provs := p.cfg.Delivery.Uploads
	p.mu.Lock()
	for _, base := range provs {
		if !slices.ContainsFunc(p.st.Uploads, func(u UploadRecord) bool { return u.Root == rootHex && u.Provider == base }) {
			p.st.Uploads = append(p.st.Uploads, UploadRecord{Root: rootHex, Provider: base, Retention: retention, Created: p.cfg.Now().Unix()})
		}
	}
	_ = p.saveLocked()
	p.mu.Unlock()
	// Every provider at once, each on a session of its own.
	out := make([]UploadResult, len(provs))
	var wg sync.WaitGroup
	for i, base := range provs {
		wg.Add(1)
		go func(i int, base string) {
			defer wg.Done()
			st, n, err := p.uploadTo(ctx, base, root, c, retention)
			out[i] = UploadResult{Provider: base, Status: st, Staged: n, Err: err}
			p.settleUpload(rootHex, base, err)
		}(i, base)
	}
	wg.Wait()
	return out
}

func (p *Publisher) settleUpload(rootHex, base string, err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for i := range p.st.Uploads {
		u := &p.st.Uploads[i]
		if u.Root != rootHex || u.Provider != base {
			continue
		}
		if err == nil {
			p.st.Uploads = slices.Delete(p.st.Uploads, i, i+1)
		} else {
			u.Tries++
			u.Last = firstLine(err.Error())
		}
		break
	}
	_ = p.saveLocked()
}

// uploadTo runs one upload session at a provider's public URL and returns
// the commit's status and how many objects were sent.
func (p *Publisher) uploadTo(ctx context.Context, base string, root [32]byte, c Closure, retention uint64) (int, int, error) {
	base = strings.TrimRight(base, "/") + "/chirp/v1/uploads"
	objs := c.Objects()
	var size *string
	if len(objs) > 0 {
		if rb, err := c.Object(ctx, root); err == nil {
			if r, err := chirp.DecodeRoot(rb); err == nil {
				s := strconv.FormatUint(r.Length, 10)
				size = &s
			}
		}
	}
	open, _ := json.Marshal(map[string]any{"retentionSeconds": strconv.FormatUint(max(retention, 1), 10), "logicalLength": size})
	call, err := p.auth.do(ctx, http.MethodPost, base, map[string]string{"content-type": "application/json"}, open, 0, 64<<10)
	if err != nil {
		return 0, 0, fmt.Errorf("open a session: %w", err)
	}
	if call.Status != http.StatusCreated {
		return call.Status, 0, fmt.Errorf("open a session: %d %s", call.Status, firstLine(string(call.Body)))
	}
	var s struct {
		UploadID string `json:"uploadId"`
	}
	if err := json.Unmarshal(call.Body, &s); err != nil || s.UploadID == "" || strings.ContainsAny(s.UploadID, "/?#") {
		return 0, 0, errors.New("open a session: no upload id")
	}
	sess := base + "/" + s.UploadID
	// The objects go uploadLanes at a time, each lane over a BRC-104
	// session of its own (one session's requests are made one at a time).
	// A fresh upload session holds nothing, so no object is asked about
	// first: a PUT of an object the provider already stores answers 204.
	jobs := make(chan [32]byte)
	var mu sync.Mutex
	var first error
	firstStatus, sent := 0, 0
	wctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var wg sync.WaitGroup
	for range min(uploadLanes, len(objs)) {
		lane := newAuthClient(p, p.cfg.Delivery.HTTP)
		wg.Add(1)
		go func() {
			defer wg.Done()
			for h := range jobs {
				st, err := p.uploadObject(wctx, lane, sess, c, h)
				mu.Lock()
				if err != nil && first == nil {
					first, firstStatus = err, st
					cancel()
				} else if err == nil {
					sent++
				}
				mu.Unlock()
			}
		}()
	}
	for _, h := range objs {
		select {
		case jobs <- h:
			continue
		case <-wctx.Done():
		}
		break
	}
	close(jobs)
	wg.Wait()
	if first != nil {
		return firstStatus, sent, first
	}
	if err := ctx.Err(); err != nil {
		return 0, sent, err
	}
	commit, _ := json.Marshal(map[string]string{"rootIdentifier": chirp.Identifier(root)})
	call, err = p.auth.do(ctx, http.MethodPost, sess+"/commit", map[string]string{"content-type": "application/json"}, commit, 0, 64<<10)
	if err != nil {
		return 0, sent, fmt.Errorf("commit: %w", err)
	}
	if call.Status != http.StatusCreated && call.Status != http.StatusAccepted {
		return call.Status, sent, fmt.Errorf("commit: %d %s", call.Status, firstLine(string(call.Body)))
	}
	return call.Status, sent, nil
}

// uploadLanes is how many objects one upload session sends at once.
const uploadLanes = 4

// uploadObject sends one object to a session, retrying a server error or a
// rate limit twice.
func (p *Publisher) uploadObject(ctx context.Context, a *authClient, sess string, c Closure, h [32]byte) (int, error) {
	url := sess + "/objects/" + chirp.Identifier(h)
	b, err := c.Object(ctx, h)
	if err != nil {
		return 0, fmt.Errorf("object %x: %w", h[:6], err)
	}
	var put *Call
	for try := 0; try < 3; try++ {
		if put, err = a.do(ctx, http.MethodPut, url, map[string]string{"content-type": "application/octet-stream"}, b, 0, 64<<10); err == nil && put.Status < 500 && put.Status != http.StatusTooManyRequests {
			break
		}
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-time.After(time.Duration(try+1) * time.Second):
		}
	}
	if err != nil {
		return 0, fmt.Errorf("object %x: %w", h[:6], err)
	}
	if put.Status != http.StatusCreated && put.Status != http.StatusNoContent {
		return put.Status, fmt.Errorf("object %x: %d %s", h[:6], put.Status, firstLine(string(put.Body)))
	}
	return put.Status, nil
}

// RetryUploads sends every closure still owed to a provider again, from
// the local store; one owed a week is given up, and said so.
func (p *Publisher) RetryUploads(ctx context.Context) int {
	p.mu.Lock()
	owed := slices.Clone(p.st.Uploads)
	p.mu.Unlock()
	left := 0
	for _, u := range owed {
		if ctx.Err() != nil {
			return left
		}
		if p.cfg.Now().Sub(time.Unix(u.Created, 0)) > outboxKeep {
			p.cfg.Logf("closure %s: given up at %s after a week", short(u.Root), u.Provider)
			p.settleUpload(u.Root, u.Provider, nil)
			continue
		}
		root, err := parseHash(u.Root)
		if err != nil {
			p.settleUpload(u.Root, u.Provider, nil)
			continue
		}
		m, err := p.cfg.Store.Manifest(root)
		if err != nil {
			p.cfg.Logf("closure %s: owed to %s and no longer in the local store: %v", short(u.Root), u.Provider, err)
			left++
			continue
		}
		_, _, err = p.uploadTo(ctx, u.Provider, root, StoreClosure{Store: p.cfg.Store, Manifest: m}, u.Retention)
		p.settleUpload(u.Root, u.Provider, err)
		if err != nil {
			p.cfg.Logf("closure %s: not taken by %s: %v", short(u.Root), u.Provider, err)
			left++
		}
	}
	return left
}

// OwedUploads is how many closures are owed to a provider.
func (p *Publisher) OwedUploads() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.st.Uploads)
}

func firstLine(s string) string {
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		s = s[:i]
	}
	if len(s) > 200 {
		s = s[:200]
	}
	return s
}
