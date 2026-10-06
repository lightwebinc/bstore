package provider

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"io"
	"net/http"
	"path/filepath"
	"strings"
	"testing"
	"time"

	authhttp "github.com/bsv-blockchain/go-sdk/auth/clients/authhttp"

	"github.com/lightwebinc/bcommon/chirp"

	"github.com/lightwebinc/bstore/internal/testwriter"
	"github.com/lightwebinc/bstore/storerec"
)

// One funding output pays for one carrier: a second, different carrier on
// it is refused, on the blob topic and on the records topic.
func TestOneCarrierPerFundingOutput(t *testing.T) {
	r := newRig(t)
	ctx := context.Background()
	p := r.start(r.config(filepath.Join(t.TempDir(), "p")), true)
	base := "https://" + p.Addr()
	tree, err := r.writer.Tree(ctx, storerec.PieceDerivation, 1)
	if err != nil {
		t.Fatal(err)
	}
	blob := storerec.BlobTopic(testBucket)
	for i, data := range [][]byte{[]byte("first piece"), []byte("second piece")} {
		pc := &storerec.Piece{Bucket: testBucket, Writer: r.writer.Identity(), Root: sha256.Sum256([]byte("r")), Piece: data}
		rec, _ := pc.Encode()
		beef, _, err := r.writer.Carrier(ctx, storerec.PieceDerivation, rec, tree, 0)
		if err != nil {
			t.Fatal(err)
		}
		ans, err := r.writer.Submit(ctx, base, blob, beef)
		if err != nil {
			t.Fatal(err)
		}
		ok, why := testwriter.Admitted(ans, blob)
		if i == 0 && !ok || i == 1 && (ok || !strings.Contains(why, "funding")) {
			t.Fatalf("carrier %d: %v %q", i, ok, why)
		}
		if i == 0 {
			// The same carrier again is the same carrier: taken.
			if ans, _ = r.writer.Submit(ctx, base, blob, beef); func() bool { ok, _ := testwriter.Admitted(ans, blob); return !ok }() {
				t.Fatal("the same carrier sent again refused")
			}
		}
	}
}

// An entry dated ahead of the provider's clock commits it to nothing; a
// closure no entry was taken for leaves nothing in the store; a renewal is
// never free.
func TestEntryRefusalsLeaveNothing(t *testing.T) {
	r := newRig(t)
	ctx := context.Background()
	cfg := r.config(filepath.Join(t.TempDir(), "p"))
	p := r.start(cfg, true)
	ahead := bounty
	ahead.Created = time.Now().Add(48 * time.Hour)
	put := r.put(ctx, []*Provider{p}, content(storerec.BlobSize+3, 2), "ahead", ahead)
	r.eventually("declined", 10*time.Second, func() bool {
		p.mu.Lock()
		defer p.mu.Unlock()
		return strings.Contains(p.st.Entries[put.C].Decision, "ahead of this provider's clock")
	})
	// The closure was checked and nothing took it: none of its objects is
	// left, in the store or in the export.
	r.eventually("dropped", 10*time.Second, func() bool {
		for _, o := range put.Object.Built.Objects() {
			if p.store.Has(sha256.Sum256(o)) {
				return false
			}
		}
		return true
	})
	res, err := r.client.Post(cfg.FrontURL()+"/renew", "application/json", strings.NewReader(`{"root":"`+strings.Repeat("0", 64)+`","until":1}`))
	if err != nil {
		t.Fatal(err)
	}
	res.Body.Close()
	if res.StatusCode == 200 {
		t.Fatal("an unauthenticated renewal answered 200")
	}
}

// A commit is bounded: the root must be this session's, and its logical
// length within the provider's object bound, before anything is walked.
func TestUploadCommitBounded(t *testing.T) {
	r := newRig(t)
	ctx := context.Background()
	cfg := r.config(filepath.Join(t.TempDir(), "p"))
	cfg.Policy.MaxObject = 16 << 20
	p := r.start(cfg, true)
	_ = p
	af := authhttp.New(r.writer.W, authhttp.WithoutLogging(), authhttp.WithHttpClient(r.client))
	call := func(method, url string, body []byte, ct string) (int, string) {
		res, err := af.Fetch(ctx, url, &authhttp.SimplifiedFetchRequestOptions{Method: method, Headers: map[string]string{"content-type": ct}, Body: body})
		if err != nil {
			t.Fatal(err)
		}
		defer res.Body.Close()
		b, _ := io.ReadAll(res.Body)
		return res.StatusCode, string(b)
	}
	up := cfg.PublicURL + "/chirp/v1/uploads"
	st, b := call("POST", up, []byte(`{"retentionSeconds":"60","logicalLength":null}`), "application/json")
	if st != 201 {
		t.Fatalf("open: %d %s", st, b)
	}
	var s struct {
		UploadID string `json:"uploadId"`
	}
	_ = json.Unmarshal([]byte(b), &s)
	// A root whose children name one 4 MiB blob eight times: 32 MiB of
	// logical length, over the bound.
	blob := content(storerec.BlobSize, 6)
	bh := sha256.Sum256(blob)
	var kids []chirp.Child
	for range 8 {
		kids = append(kids, chirp.Child{Kind: chirp.ChildBlob, Length: storerec.BlobSize, Hash: bh})
	}
	root, err := chirp.EncodeRoot(chirp.Root{Profile: chirp.Profile1, Length: 8 * storerec.BlobSize, ContentHash: sha256.Sum256(nil), Children: kids})
	if err != nil {
		t.Fatal(err)
	}
	rh := sha256.Sum256(root)
	commit := []byte(`{"rootIdentifier":"` + chirp.Identifier(rh) + `"}`)
	if st, b := call("POST", up+"/"+s.UploadID+"/commit", commit, "application/json"); st != 422 || !strings.Contains(b, "not uploaded in this session") {
		t.Fatalf("a root not staged: %d %s", st, b)
	}
	for _, ob := range [][]byte{blob, root} {
		if st, b := call("PUT", up+"/"+s.UploadID+"/objects/"+chirp.Identifier(sha256.Sum256(ob)), ob, "application/octet-stream"); st != 201 {
			t.Fatalf("put: %d %s", st, b)
		}
	}
	start := time.Now()
	if st, b := call("POST", up+"/"+s.UploadID+"/commit", commit, "application/json"); st != 422 || !strings.Contains(b, "larger than this provider takes") {
		t.Fatalf("an oversized root: %d %s", st, b)
	}
	if time.Since(start) > 5*time.Second {
		t.Fatal("the oversized root was walked")
	}
	_ = http.StatusOK
}
