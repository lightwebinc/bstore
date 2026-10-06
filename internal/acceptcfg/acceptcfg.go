// Package acceptcfg is the payment acceptance settings every bstore
// configuration file names (bcommon acceptance.Policy): the writer's home,
// the gateway's and the provider's, the same keys in each.
package acceptcfg

import (
	"time"

	"github.com/lightwebinc/bcommon/acceptance"
)

// Config is embedded in a configuration file's struct, so its keys sit
// beside the file's own.
type Config struct {
	// ThresholdSats is the largest payment taken on the network's
	// acceptance; above it a payment waits for its block. Unset is
	// acceptance.DefaultThresholdSats; zero holds every payment.
	ThresholdSats *uint64 `json:"accept_threshold_sats,omitempty"`
	// PayerLimit and TotalLimit bound the satoshis taken fast and not yet
	// mined, per payer and in all; zero is one and ten thresholds.
	PayerLimit uint64 `json:"accept_payer_limit,omitempty"`
	TotalLimit uint64 `json:"accept_total_limit,omitempty"`
	// WindowSeconds is how long a fast payment counts against the limits
	// unless it mines first; zero is an hour. WatchSeconds is how long to
	// watch for a conflict after the network's acceptance; zero is none.
	WindowSeconds uint64 `json:"accept_window_seconds,omitempty"`
	WatchSeconds  uint64 `json:"accept_watch_seconds,omitempty"`
}

// AcceptPolicy is the rule the settings name.
func (c Config) AcceptPolicy() acceptance.Policy {
	p := acceptance.DefaultPolicy()
	if c.ThresholdSats != nil {
		p.ThresholdSats = *c.ThresholdSats
	}
	p.PayerLimit, p.TotalLimit = c.PayerLimit, c.TotalLimit
	if p.PayerLimit == 0 {
		p.PayerLimit = p.ThresholdSats
	}
	if p.TotalLimit == 0 {
		p.TotalLimit = acceptance.DefaultTotalFactor * p.ThresholdSats
	}
	if c.WindowSeconds > 0 {
		p.Window = time.Duration(c.WindowSeconds) * time.Second
	}
	p.Watch = time.Duration(c.WatchSeconds) * time.Second
	return p
}
