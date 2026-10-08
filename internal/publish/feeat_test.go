package publish

import (
	"testing"

	"github.com/lightwebinc/bcommon/mint"
)

// The transactions built here (hold outputs, the payment coin split, a
// settlement, a reclaim, a sweep's fee coin) price a size as mint does:
// the rate capped at MaxRate, at least the floor, never the two added; and
// change under Dust is not kept.
func TestFeeAtFloorAndCap(t *testing.T) {
	legacy := mint.DefaultFees // fee_sat_per_byte 1, fee_floor 250 read over the defaults
	legacy.Rate, legacy.Floor = mint.Rate{Sats: 1, Bytes: 1}, 250
	for _, c := range []struct {
		name string
		fees mint.Fees
		size uint64
		want uint64
		dust uint64
	}{
		{"default small", mint.DefaultFees, 200 + 45, 100, 100},
		{"default at 1000", mint.DefaultFees, 1000, 100, 100},
		{"default large", mint.DefaultFees, 13_300, 1330, 100},
		{"legacy rate capped small", legacy, 245, 250, 100},
		{"legacy rate capped large", legacy, 13_300, 1330, 100},
		{"live rate above cap", mint.Fees{Rate: mint.Rate{Sats: 500, Bytes: 1000}, Floor: 100, Dust: 100, MaxRate: mint.DefaultFees.MaxRate}, 5000, 500, 100},
		{"dust unset is the floor", mint.Fees{Rate: mint.Rate{Sats: 100, Bytes: 1000}, Floor: 120}, 10, 120, 120},
	} {
		if got := feeAt(c.fees, c.size); got != c.want {
			t.Errorf("%s: fee %d, want %d", c.name, got, c.want)
		}
		if got := dustOf(c.fees); got != c.dust {
			t.Errorf("%s: dust %d, want %d", c.name, got, c.dust)
		}
		if f, err := c.fees.For(int(c.size)); err != nil || f != c.want { //nolint:gosec // small
			t.Errorf("%s: mint prices %d (%v), here %d", c.name, f, err, c.want)
		}
	}
}
