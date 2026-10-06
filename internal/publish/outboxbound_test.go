package publish

import (
	"errors"
	"testing"
	"time"
)

// Past its bound the outbox refuses a put, naming the target owing most;
// under it, or empty, it does not.
func TestOutboxRoom(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	o, err := openOutbox(t.TempDir(), &Delivery{}, nil, func(string, ...any) {}, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	o.Max = 1000
	if err := o.room(5000); err != nil {
		t.Fatalf("an empty outbox refused: %v", err)
	}
	for i, tg := range []string{"http://p1", "http://p2"} {
		it, err := o.add([]string{"aa", "bb"}[i], "piece", make([]byte, 600), []Target{{Base: tg}})
		if err != nil {
			t.Fatal(err)
		}
		o.done(it.ID)
	}
	now = now.Add(time.Minute)
	err = o.room(10)
	if !errors.Is(err, ErrOutboxFull) {
		t.Fatalf("got %v", err)
	}
	total, by := o.Owed()
	if total != 1200 || by["http://p1"] != 600 {
		t.Fatalf("owed %d %v", total, by)
	}
	o.Max = 2000
	if err := o.room(10); err != nil {
		t.Fatal(err)
	}
}
