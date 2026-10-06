package s3d

import (
	"strings"
	"testing"
)

func minimalConfig() *Config {
	return &Config{Data: "/var/lib/bstore-s3d", Node: "http://node", Settle: "arcade:http://arcade", Headers: "http://headers",
		Hosts: []string{"http://host"}, Providers: []string{"https://provider"}, Identities: []string{"gw"},
		AccessKeys: []AccessKey{{ID: "KEY01", Secret: strings.Repeat("s", 16), Identity: "gw"}},
		Buckets: []BucketConfig{{Bucket: "photos_qzvmkwerty", Owner: "02c6047f9441ed7d6d3045406e95c07cd85c778e4b8cef3ca7abac09b95c709ee5",
			ID: strings.Repeat("ab", 32)}}}
}

// The defaults: a piece tree that holds the largest PutObject's pieces (at
// most 1,023), one tree always ready, four workers, the race on.
func TestConfigDefaults(t *testing.T) {
	c := minimalConfig()
	if err := c.Check(); err != nil {
		t.Fatal(err)
	}
	if c.PieceTree != 1023 || c.PieceAhead != 1023 || c.PieceWorkers != 4 || c.EntryTree != 16 {
		t.Fatalf("piece_tree %d piece_ahead %d piece_workers %d entry_tree %d", c.PieceTree, c.PieceAhead, c.PieceWorkers, c.EntryTree)
	}
	if c.Race.Off || c.Race.WindowSeconds != 3600 || c.Race.TickSeconds != 10 {
		t.Fatalf("race %+v", c.Race)
	}
	c = minimalConfig()
	c.Limits.MaxObject = 100 << 20
	if err := c.Check(); err != nil || c.PieceTree != 64 {
		t.Fatalf("100 MiB puts: piece_tree %d (%v)", c.PieceTree, err)
	}
	c = minimalConfig()
	c.Limits.MaxObject = 1 << 30
	if err := c.Check(); err != nil || c.PieceTree != 257 {
		t.Fatalf("1 GiB puts: piece_tree %d (%v)", c.PieceTree, err)
	}
	c = minimalConfig()
	c.PieceTree, c.PieceAhead = 32, 8
	if err := c.Check(); err != nil || c.PieceTree != 32 || c.PieceAhead != 8 {
		t.Fatalf("set: %d %d (%v)", c.PieceTree, c.PieceAhead, err)
	}
	for _, bad := range []func(*Config){
		func(c *Config) { c.PieceWorkers = 33 },
		func(c *Config) { c.PieceTree = 1024 },
		func(c *Config) { c.Race.TickSeconds = -1 },
	} {
		c := minimalConfig()
		bad(c)
		if c.Check() == nil {
			t.Fatalf("accepted %+v", c)
		}
	}
}
