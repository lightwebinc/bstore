package publish

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lightwebinc/bcommon/headers"

	"github.com/lightwebinc/bstore/storerec"
)

// gateTip counts the steps reading the tip at once, and holds each one long
// enough for the others to arrive.
type gateTip struct {
	mu        sync.Mutex
	now, most int
}

func (g *gateTip) CurrentHeight(context.Context) (uint32, error) {
	g.mu.Lock()
	g.now++
	g.most = max(g.most, g.now)
	g.mu.Unlock()
	time.Sleep(50 * time.Millisecond)
	g.mu.Lock()
	g.now--
	g.mu.Unlock()
	return 100, nil
}

func (g *gateTip) HeaderAt(context.Context, uint32) (*headers.Header, error) { return nil, nil }

// A pass steps up to Parallel open races at once, and every one of them; one
// at a time without it. Each race here is past its deadline, so its step ends
// after reading the tip, lapsed.
func TestStepOpenRunsParallel(t *testing.T) {
	for _, tc := range []struct{ parallel, want int }{{0, 1}, {1, 1}, {4, 4}} {
		t.Run(fmt.Sprint(tc.parallel), func(t *testing.T) {
			dir := t.TempDir()
			if err := os.MkdirAll(filepath.Join(dir, "tmp"), 0o700); err != nil {
				t.Fatal(err)
			}
			tip := &gateTip{}
			p := &Publisher{cfg: Config{Dir: dir, Tip: tip, Logf: t.Logf, Now: time.Now}, st: &state{}}
			var races []*Race
			for i := range 10 {
				races = append(races, &Race{Entry: strings.Repeat(fmt.Sprintf("%02x", i), 32), State: RaceOpen,
					entry: &storerec.Entry{Hold: &storerec.Hold{Deadline: 50}}})
			}
			// Settled races and races with a settlement are not stepped.
			races = append(races, &Race{Entry: strings.Repeat("ee", 32), State: RaceSettled},
				&Race{Entry: strings.Repeat("ff", 32), State: RaceOpen, Settlement: &SettleRecord{}})
			p.stepOpen(context.Background(), races, RaceOptions{Parallel: tc.parallel})
			if tip.most != tc.want {
				t.Fatalf("at most %d steps at once, want %d", tip.most, tc.want)
			}
			for _, r := range races[:10] {
				if r.State != RaceLapsed {
					t.Fatalf("race %s is %v, want lapsed", short(r.Entry), r.State)
				}
			}
			for _, r := range races[10:] {
				if r.State == RaceLapsed {
					t.Fatalf("race %s was stepped", short(r.Entry))
				}
			}
		})
	}
}
