package reader

import (
	"bytes"
	"context"
	"testing"
	"time"
)

// A whole read of a small object must end. The content hash runs on a
// goroutine of its own; it once took its channel from a variable the
// writing loop set to nil after the last blob, so a hasher that had not
// started by then waited on a nil channel forever and the read with it.
func TestWholeReadOfASmallObjectEnds(t *testing.T) {
	content := []byte("a small object, one blob")
	o, _ := testObject(t, content)
	done := make(chan error, 1)
	go func() {
		for i := 0; i < 20000; i++ {
			var out bytes.Buffer
			if err := o.WriteRange(context.Background(), &out, 0, o.entry.Size); err != nil {
				done <- err
				return
			}
			if !bytes.Equal(out.Bytes(), content) {
				t.Errorf("read %d differs", i)
			}
		}
		done <- nil
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(60 * time.Second):
		t.Fatal("a whole read of a small object did not end")
	}
}
