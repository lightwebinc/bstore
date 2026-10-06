package acceptcfg

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/lightwebinc/bcommon/acceptance"
)

func TestPolicy(t *testing.T) {
	var c Config
	p := c.AcceptPolicy()
	if p.ThresholdSats != acceptance.DefaultThresholdSats || p.PayerLimit != p.ThresholdSats || p.TotalLimit != 10*p.ThresholdSats || p.Window != time.Hour || p.Watch != 0 {
		t.Fatalf("defaults: %+v", p)
	}
	if err := json.Unmarshal([]byte(`{"accept_threshold_sats":0}`), &c); err != nil {
		t.Fatal(err)
	}
	if p := c.AcceptPolicy(); p.ThresholdSats != 0 || p.PayerLimit != 0 || p.TotalLimit != 0 {
		t.Fatalf("zero threshold holds everything: %+v", p)
	}
	if err := json.Unmarshal([]byte(`{"accept_threshold_sats":1000,"accept_payer_limit":3000,"accept_window_seconds":60,"accept_watch_seconds":2}`), &c); err != nil {
		t.Fatal(err)
	}
	if p := c.AcceptPolicy(); p.ThresholdSats != 1000 || p.PayerLimit != 3000 || p.TotalLimit != 10000 || p.Window != time.Minute || p.Watch != 2*time.Second {
		t.Fatalf("set: %+v", p)
	}
}
