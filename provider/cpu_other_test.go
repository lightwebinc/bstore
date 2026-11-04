//go:build !unix

package provider

import "time"

// processCPU is zero where the process's CPU time is not read.
func processCPU() time.Duration { return 0 }
