package chaincfg

import (
	"context"
	"testing"

	"github.com/lightwebinc/bcommon/mint"
)

// The older keys fee_sat_per_byte and fee_floor are still read, over
// mint.DefaultFees: the rate they set is capped at the default max rate
// (100/1000), and the dust and the floor default to 100.
func TestLegacyFeeKeysCapped(t *testing.T) {
	for _, c := range []struct {
		spec       Spec
		floor      uint64
		fee1, fee5 uint64 // the fee at 1,000 and 5,000 bytes
	}{
		{Spec{Network: "main"}, 100, 100, 500},
		{Spec{Network: "main", FeeRate: 1}, 100, 100, 500},
		{Spec{Network: "main", FeeRate: 1, FeeFloor: 250}, 250, 250, 500},
		{Spec{Network: "main", FeeFloor: 300}, 300, 300, 500},
	} {
		f, err := c.spec.Fees()
		if err != nil {
			t.Fatal(err)
		}
		if f.Floor != c.floor || f.Dust != 100 || f.MaxRate != (mint.Rate{Sats: 100, Bytes: 1000}) {
			t.Fatalf("%+v: fees %+v", c.spec, f)
		}
		for _, s := range []struct {
			size int
			want uint64
		}{{1000, c.fee1}, {5000, c.fee5}} {
			if got, err := f.For(s.size); err != nil || got != s.want {
				t.Fatalf("%+v: fee at %d bytes %d (%v), want %d", c.spec, s.size, got, err, s.want)
			}
		}
	}
	// What doctor prints is the rate paid, the cap applied.
	v, err := Open(Spec{Network: "main", FeeRate: 1})
	if err != nil {
		t.Fatal(err)
	}
	if l := v.feeLine(context.Background()); l.Text != "100/1000 satoshis/bytes, floor 100, static" {
		t.Fatalf("doctor fee line %q", l.Text)
	}
}
