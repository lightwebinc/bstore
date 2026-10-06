package s3d

import (
	"testing"
	"time"

	"github.com/lightwebinc/bstore/storerec"
)

// A put sent again with the same content, type, ETag and metadata by the
// same writer, while its entry is the key's newest and recent, is answered
// by that entry; anything else is a new write.
func TestRetryOfSameWrite(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	clock := func() time.Time { return now }
	st, err := openState(t.TempDir(), clock)
	if err != nil {
		t.Fatal(err)
	}
	id, writer, root := [32]byte{1}, []byte{2, 3}, [32]byte{9}
	etag := "abc"
	meta := []storerec.MetaPair{{Key: "b", Value: "2"}, {Key: "a", Value: "1"}}
	e := &storerec.Entry{Bucket: "bk", ID: id, Writer: writer, Key: "k", Kind: storerec.KindPut, Created: 5, Size: 10, Root: root,
		ContentType: "text/plain", ETag: &etag, Meta: meta}
	if err := st.add(e, []byte{0}, [32]byte{4}); err != nil {
		t.Fatal(err)
	}
	same := []storerec.MetaPair{{Key: "a", Value: "1"}, {Key: "b", Value: "2"}}
	if st.retryOf("bk", id, writer, "k", "text/plain", same, "abc", root, 10, now) == nil {
		t.Fatal("the same write was not recognised")
	}
	for name, got := range map[string]*storerec.Entry{
		"other content": st.retryOf("bk", id, writer, "k", "text/plain", same, "abc", [32]byte{8}, 10, now),
		"other type":    st.retryOf("bk", id, writer, "k", "text/html", same, "abc", root, 10, now),
		"other meta":    st.retryOf("bk", id, writer, "k", "text/plain", meta[:1], "abc", root, 10, now),
		"other writer":  st.retryOf("bk", id, []byte{7}, "k", "text/plain", same, "abc", root, 10, now),
		"other key":     st.retryOf("bk", id, writer, "j", "text/plain", same, "abc", root, 10, now),
		"too late":      st.retryOf("bk", id, writer, "k", "text/plain", same, "abc", root, 10, now.Add(2*time.Hour)),
	} {
		if got != nil {
			t.Errorf("%s: taken as a retry", name)
		}
	}
	// A newer write to the key in between: the retry is a new write.
	e2 := *e
	e2.Created, e2.Root = 6, [32]byte{8}
	if err := st.add(&e2, []byte{0}, [32]byte{5}); err != nil {
		t.Fatal(err)
	}
	if st.retryOf("bk", id, writer, "k", "text/plain", same, "abc", root, 10, now) != nil {
		t.Fatal("a retry past a newer write was answered by the older entry")
	}
}
