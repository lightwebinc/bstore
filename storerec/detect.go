package storerec

import (
	"fmt"
	"math"
	"strconv"
	"strings"
)

// DetectionCs and DetectionLosses are the rows and columns of spec section
// 7.6's first table; ConfidenceLevels the columns of its second.
var (
	DetectionCs      = []int{1, 8, 16, 30, 60, 100, 230, 459, 1000}
	DetectionLosses  = []float64{0.5, 0.25, 0.1, 0.05, 0.01, 0.001}
	ConfidenceLevels = []float64{0.9, 0.99, 0.999}
)

// Detect is the probability that one challenge of c samples catches a
// provider that lacks a fraction f of the segments: 1 - (1 - f)^c.
func Detect(c int, f float64) float64 { return 1 - math.Pow(1-f, float64(c)) }

// SamplesFor is the least c that catches a loss of f with probability at
// least conf: ceil(ln(1 - conf) / ln(1 - f)).
func SamplesFor(f, conf float64) int {
	c := int(math.Ceil(math.Log(1-conf) / math.Log(1-f)))
	for c > 1 && Detect(c-1, f) >= conf {
		c--
	}
	for Detect(c, f) < conf {
		c++
	}
	return c
}

func pct(p float64) string {
	v := math.Round(p*1000) / 10
	if v > 99.9 {
		return ">99.9"
	}
	return strconv.FormatFloat(v, 'f', 1, 64)
}

func lossLabel(f float64) string {
	return strconv.FormatFloat(f*100, 'f', -1, 64) + " %"
}

func thousands(n int) string {
	s := strconv.Itoa(n)
	var b strings.Builder
	for i, c := range s {
		if i > 0 && (len(s)-i)%3 == 0 {
			b.WriteByte(',')
		}
		b.WriteRune(c)
	}
	return b.String()
}

// DetectionTables writes spec section 7.6's two tables as the spec prints
// them, so that a test holds the spec's figures to the formula.
func DetectionTables() (string, string) {
	var a strings.Builder
	a.WriteString("| `c` |")
	for i, f := range DetectionLosses {
		if i == 0 {
			a.WriteString(" " + lossLabel(f) + " lost |")
		} else {
			a.WriteString(" " + lossLabel(f) + " |")
		}
	}
	a.WriteString("\n|" + strings.Repeat(" --- |", len(DetectionLosses)+1) + "\n")
	for _, c := range DetectionCs {
		fmt.Fprintf(&a, "| %d |", c)
		for _, f := range DetectionLosses {
			a.WriteString(" " + pct(Detect(c, f)) + " |")
		}
		a.WriteString("\n")
	}
	var s strings.Builder
	s.WriteString("| Lost |")
	for _, conf := range ConfidenceLevels {
		s.WriteString(" " + strconv.FormatFloat(conf*100, 'f', -1, 64) + " % |")
	}
	s.WriteString("\n|" + strings.Repeat(" --- |", len(ConfidenceLevels)+1) + "\n")
	for _, f := range DetectionLosses {
		s.WriteString("| " + lossLabel(f) + " |")
		for _, conf := range ConfidenceLevels {
			s.WriteString(" " + thousands(SamplesFor(f, conf)) + " |")
		}
		s.WriteString("\n")
	}
	return a.String(), s.String()
}
