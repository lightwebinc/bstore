package reader

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/lightwebinc/bcommon/chaintoken"
	"github.com/lightwebinc/bcommon/chirp"

	"github.com/lightwebinc/bstore/internal/storerec"
)

// Source is an advertiser of a root: a BRC-26 advertisement verified
// against the reader's headers, unexpired, of the CHIRP form for the root,
// by a key that is not the anyone key.
type Source struct {
	Advertiser string // 66 hex
	URL        string
	Expiry     uint64
	Txid       string
}

// objectURL is the BRC-167 object route for h at the source's host.
func (s Source) objectURL(h [32]byte) string {
	i := strings.LastIndex(s.URL, "/")
	return s.URL[:i+1] + chirp.Identifier(h)
}

// maxAdPages bounds the ads pages asked per root (64 a page).
const maxAdPages = 4

// Sources are root's advertisers as the hosts answer them, one per
// advertiser (its latest expiry), latest expiry first. A host's answer is
// kept for a short while.
func (r *Reader) Sources(ctx context.Context, bucket string, root [32]byte) []Source {
	if s, ok := r.ads.get(root, r.now()); ok {
		return s
	}
	best := map[string]Source{}
	for _, h := range r.Hosts {
		after := ""
		for page := 0; page < maxAdPages; page++ {
			a, err := r.ask(ctx, h, map[string]string{"bucket": bucket, "root": hex.EncodeToString(root[:]), "after": after})
			if err != nil {
				r.logf("%v", &hostError{h, fmt.Errorf("ads: %w", err)})
				break
			}
			last := ""
			for _, o := range a.Outputs {
				tx, err := subject(o.Beef, o.OutputIndex)
				if err != nil {
					continue
				}
				last = tx.TxID().String()
				ad, err := storerec.DecodeAdvertisement(*tx.Outputs[o.OutputIndex].LockingScript)
				if err != nil || ad.Hash != root || ad.Expiry <= uint64(r.now().Unix()) ||
					!strings.HasSuffix(ad.URL, storerec.CHIRPURLSuffix(root)) || !strings.HasPrefix(ad.URL, "https://") ||
					string(ad.Host) == string(storerec.AnyoneKey) {
					continue
				}
				if !chaintoken.Mined(ctx, tx, r.Headers) {
					continue
				}
				k := hex.EncodeToString(ad.Host)
				if cur, ok := best[k]; !ok || ad.Expiry > cur.Expiry {
					best[k] = Source{Advertiser: k, URL: ad.URL, Expiry: ad.Expiry, Txid: last}
				}
			}
			if len(a.Outputs) < 64 || last == "" {
				break
			}
			after = last
		}
	}
	out := make([]Source, 0, len(best))
	for _, s := range best {
		out = append(out, s)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Expiry != out[j].Expiry {
			return out[i].Expiry > out[j].Expiry
		}
		return out[i].Advertiser < out[j].Advertiser
	})
	r.ads.put(root, out, r.now())
	return out
}

// Local is a reader's own copy of closures, tried before any advertiser.
type Local interface {
	Get(h [32]byte) ([]byte, error)
}

// Object is an object's closure opened for reading: the root and every
// branch fetched and checked, the blob references in content order.
type Object struct {
	r       *Reader
	entry   *storerec.Entry
	blobs   []chirp.Child
	nodes   [][32]byte // the root and the branches, each once
	sources []Source
	local   Local
}

// Size is the object's length.
func (o *Object) Size() uint64 { return o.entry.Size }

// Open reads an entry's root and branches (spec section 15.2 steps 6 and
// 7 for the nodes): each held to its hash, the root to bstore's root rules
// (CheckRoot, rows C1 and C2), each branch decoded and its length the
// reference's, and the whole tree held to profile 1's canonical
// construction over the blob references by rebuilding it. Blobs are read
// later, each held to its hash before a byte of it is released.
func (r *Reader) Open(ctx context.Context, e *storerec.Entry, local Local) (*Object, error) {
	o := &Object{r: r, entry: e, local: local}
	if e.Size == 0 {
		return o, nil
	}
	o.sources = r.Sources(ctx, e.Bucket, e.Root)
	rootBytes, err := o.fetch(ctx, e.Root, chirp.MaxNode)
	if err != nil {
		return nil, err
	}
	root, err := storerec.CheckRoot(rootBytes, e.Root, e.Size, e.ContentHash)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrRefused, err)
	}
	o.nodes = append(o.nodes, e.Root)
	seenNode := map[[32]byte]bool{e.Root: true}
	maxBlobs := storerec.Blobs(e.Size)
	var walk func(children []chirp.Child, depth int) error
	walk = func(children []chirp.Child, depth int) error {
		for _, ch := range children {
			switch ch.Kind {
			case chirp.ChildBlob:
				if uint64(len(o.blobs)) >= maxBlobs {
					return fmt.Errorf("%w: more blobs than the size holds", ErrRefused)
				}
				o.blobs = append(o.blobs, ch)
			case chirp.ChildBranch:
				if depth+1 >= chirp.MaxDepth {
					return fmt.Errorf("%w: the closure is deeper than %d", ErrRefused, chirp.MaxDepth)
				}
				b, err := o.fetch(ctx, ch.Hash, chirp.MaxNode)
				if err != nil {
					return err
				}
				if !seenNode[ch.Hash] {
					seenNode[ch.Hash] = true
					o.nodes = append(o.nodes, ch.Hash)
				}
				br, err := chirp.DecodeBranch(b)
				if err != nil || br.Length != ch.Length {
					return fmt.Errorf("%w: a branch of the closure is not valid", ErrRefused)
				}
				if err := walk(br.Children, depth+1); err != nil {
					return err
				}
			default:
				return fmt.Errorf("%w: a child of unknown kind", ErrRefused)
			}
		}
		return nil
	}
	if err := walk(root.Children, 1); err != nil {
		return nil, err
	}
	built, err := chirp.BuildTree(o.blobs, e.ContentHash, nil)
	if err != nil || built.RootHash != e.Root {
		return nil, fmt.Errorf("%w: the closure is not profile 1's canonical construction", ErrRefused)
	}
	return o, nil
}

// Objects are the closure's distinct objects: the root, the branches, then
// the blobs (a publisher's Closure).
func (o *Object) Objects() [][32]byte {
	out := append([][32]byte(nil), o.nodes...)
	seen := map[[32]byte]bool{}
	for _, h := range out {
		seen[h] = true
	}
	for _, b := range o.blobs {
		if !seen[b.Hash] {
			seen[b.Hash] = true
			out = append(out, b.Hash)
		}
	}
	return out
}

// Object is one object of the closure, held to its hash.
func (o *Object) Object(ctx context.Context, h [32]byte) ([]byte, error) {
	return o.fetch(ctx, h, chirp.ChunkSize)
}

// fetch returns the object named h, at most max bytes, from the local copy
// or the first advertiser that serves it as its hash names it. An
// advertiser serving other bytes is named in the log and skipped.
func (o *Object) fetch(ctx context.Context, h [32]byte, max uint64) ([]byte, error) {
	if o.local != nil {
		if b, err := o.local.Get(h); err == nil && sha256.Sum256(b) == h {
			return b, nil
		}
	}
	var tried int
	for _, s := range o.r.bad.order(o.sources, o.r.now()) {
		if tried == maxTries {
			break
		}
		tried++
		b, err := o.r.get(ctx, s.objectURL(h), max)
		if err != nil {
			// Any failure sends the advertiser to the back for a while, so
			// one that hangs costs a reader once, not on every blob.
			o.r.logf("advertiser %s at %s did not serve object %x: %v", short(s.Advertiser), s.URL, h[:6], err)
			o.r.bad.mark(s.Advertiser, o.r.now())
			continue
		}
		if sha256.Sum256(b) != h {
			o.r.logf("advertiser %s at %s served bytes that do not hash to object %x: skipped", short(s.Advertiser), s.URL, h[:6])
			o.r.bad.mark(s.Advertiser, o.r.now())
			continue
		}
		return b, nil
	}
	return nil, fmt.Errorf("%w: object %x (%d advertiser(s) asked)", ErrNoSource, h[:6], tried)
}

// maxTries bounds the advertisers asked for one object, and fetchTimeout
// one ask: a 4 MiB blob that takes longer is not worth waiting for.
const (
	maxTries     = 8
	fetchTimeout = time.Minute
)

// get fetches a URL's body, at most max bytes.
func (r *Reader) get(ctx context.Context, url string, max uint64) ([]byte, error) {
	ctx, cancel := context.WithTimeout(ctx, fetchTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	hc := r.Fetch
	if hc == nil {
		hc = http.DefaultClient
	}
	res, err := hc.Do(req)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("%d", res.StatusCode)
	}
	b, err := io.ReadAll(io.LimitReader(res.Body, int64(max)+1))
	if err != nil {
		return nil, err
	}
	if uint64(len(b)) > max {
		return nil, fmt.Errorf("more than %d bytes", max)
	}
	return b, nil
}

// ErrContentHash is a whole read whose content does not hash to the
// entry's contentHash (spec section 15.2 step 8): the writer's fault.
var ErrContentHash = errors.New("reader: the content does not hash to the entry's contentHash")

// WriteRange writes bytes [off, off+n) of the object to w, fetching each
// blob it overlaps whole and holding it to its hash before a byte of it is
// written (spec section 15.2). A whole read (off 0, n the size) also holds
// the content to its hash, checked before the last blob is written, so a
// whole object whose content hash fails is never released complete.
func (o *Object) WriteRange(ctx context.Context, w io.Writer, off, n uint64) error {
	if n == 0 {
		return nil
	}
	if off+n > o.entry.Size || off+n < off {
		return errors.New("reader: a range past the object")
	}
	whole := off == 0 && n == o.entry.Size
	var ch hash.Hash
	if whole {
		ch = sha256.New()
	}
	first, last := off/storerec.BlobSize, (off+n-1)/storerec.BlobSize
	for i := first; i <= last; i++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		c := o.blobs[i]
		b, err := o.fetch(ctx, c.Hash, c.Length)
		if err != nil {
			return err
		}
		if uint64(len(b)) != c.Length {
			return fmt.Errorf("%w: a blob of the wrong length", ErrRefused)
		}
		if ch != nil {
			ch.Write(b)
			if i == last && [32]byte(ch.Sum(nil)) != o.entry.ContentHash {
				return ErrContentHash
			}
		}
		start := uint64(0)
		if i == first {
			start = off - i*storerec.BlobSize
		}
		end := c.Length
		if i == last {
			end = off + n - i*storerec.BlobSize
		}
		if _, err := w.Write(b[start:end]); err != nil {
			return err
		}
	}
	return nil
}

// adCache keeps a root's sources for a short while.
type adCache struct {
	mu sync.Mutex
	m  map[[32]byte]adEntry
}

type adEntry struct {
	at  time.Time
	src []Source
}

const adTTL = 30 * time.Second

func (c *adCache) get(root [32]byte, now time.Time) ([]Source, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.m[root]
	if !ok || now.Sub(e.at) > adTTL || len(e.src) == 0 {
		return nil, false
	}
	return e.src, true
}

func (c *adCache) put(root [32]byte, s []Source, now time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.m == nil || len(c.m) >= 4096 {
		c.m = map[[32]byte]adEntry{}
	}
	c.m[root] = adEntry{now, s}
}

// Forget drops a root's cached sources.
func (r *Reader) Forget(root [32]byte) {
	r.ads.mu.Lock()
	delete(r.ads.m, root)
	r.ads.mu.Unlock()
}

// badSources remembers advertisers that served wrong bytes, for a while:
// they are asked last.
type badSources struct {
	mu sync.Mutex
	m  map[string]time.Time
}

const badTTL = 10 * time.Minute

func (b *badSources) mark(adv string, now time.Time) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.m == nil || len(b.m) >= 4096 {
		b.m = map[string]time.Time{}
	}
	b.m[adv] = now
}

func (b *badSources) order(src []Source, now time.Time) []Source {
	b.mu.Lock()
	defer b.mu.Unlock()
	var good, bad []Source
	for _, s := range src {
		if at, ok := b.m[s.Advertiser]; ok && now.Sub(at) < badTTL {
			bad = append(bad, s)
		} else {
			good = append(good, s)
		}
	}
	return append(good, bad...)
}
