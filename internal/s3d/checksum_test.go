package s3d

import (
	"bytes"
	"hash/crc64"
	"testing"
)

// crc64NVME is CRC-64/NVME as hash/crc64 takes it (the reflected polynomial
// 0x9a6c9329ac4bc9b5; initial and final value all ones): the independent
// reference the gateway's fast implementation is held to.
var crc64NVME = crc64.MakeTable(0x9a6c9329ac4bc9b5)

// TestCRC64NVMEMatchesTheTable: the gateway's crc64nvme digest is the
// table's, for lengths around every block boundary the assembly has and
// for a body fed in uneven writes, as an HTTP body arrives.
func TestCRC64NVMEMatchesTheTable(t *testing.T) {
	body := make([]byte, 1<<20+77)
	for i := range body {
		body[i] = byte(i*131 + i>>9)
	}
	lengths := []int{0, 1, 7, 8, 15, 16, 17, 63, 64, 65, 127, 128, 129, 255, 256, 257, 4095, 4096, 4097, 65536, len(body)}
	for _, n := range lengths {
		want := crc64.New(crc64NVME)
		want.Write(body[:n])
		got := checksumAlgos["crc64nvme"]()
		got.Write(body[:n])
		if !bytes.Equal(got.Sum(nil), want.Sum(nil)) {
			t.Fatalf("length %d: %x, the table %x", n, got.Sum(nil), want.Sum(nil))
		}
	}
	got := checksumAlgos["crc64nvme"]()
	steps := []int{1, 3, 17, 64, 255, 1000, 4097, 8999, 65535}
	for off, i := 0, 0; off < len(body); i++ {
		end := min(off+steps[i%len(steps)], len(body))
		got.Write(body[off:end])
		off = end
	}
	want := crc64.New(crc64NVME)
	want.Write(body)
	if !bytes.Equal(got.Sum(nil), want.Sum(nil)) {
		t.Fatalf("uneven writes: %x, the table %x", got.Sum(nil), want.Sum(nil))
	}
}
