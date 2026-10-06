package storerec

import (
	"strings"
)

// The SDK's advertisement check parses its url with the WHATWG URL parser
// (JavaScript's URL) and requires protocol "https:", a non-empty hostname,
// no username, no password and an empty hash. httpsURL decides the same for
// the strings it can decide exactly: every url whose host is ASCII after
// percent-decoding and has no label starting "xn--". A host outside that
// (an internationalised name, which the WHATWG parser maps through UTS #46)
// is refused here and reported as narrowed; the TypeScript twin, which runs
// the WHATWG parser itself, may admit it. docs/codec.md records the
// narrowing.
//
// s is the url as the SDK holds it: valid UTF-8, one leading BOM removed,
// no control character (every one is refused before the parse).
func httpsURL(s string) (ok, narrowed bool) {
	s = strings.Trim(s, " ")
	// Scheme: an ASCII letter, then letters, digits, "+", "-", "." up to ":".
	if s == "" || !isAlpha(s[0]) {
		return false, false
	}
	colon := -1
	for i := 1; i < len(s); i++ {
		c := s[i]
		if c == ':' {
			colon = i
			break
		}
		if !isAlpha(c) && !isDigit(c) && c != '+' && c != '-' && c != '.' {
			return false, false
		}
	}
	if colon < 0 || strings.ToLower(s[:colon]) != "https" {
		return false, false
	}
	rest := s[colon+1:]
	// A special scheme skips every leading "/" and "\".
	rest = strings.TrimLeft(rest, "/\\")
	// The authority runs to the first "/", "?", "#" or "\".
	end := strings.IndexAny(rest, "/?#\\")
	auth, tail := rest, ""
	if end >= 0 {
		auth, tail = rest[:end], rest[end:]
	}
	// A fragment is everything after the first "#"; an empty one is no hash.
	if i := strings.IndexByte(tail, '#'); i >= 0 && i < len(tail)-1 {
		return false, false
	}
	hostport := auth
	if at := strings.LastIndexByte(auth, '@'); at >= 0 {
		// Only "" and ":" leave both username and password empty: a second
		// "@" or ":" is percent-encoded into one of them, and anything else
		// is a character of one of them.
		if ui := auth[:at]; ui != "" && ui != ":" {
			return false, false
		}
		hostport = auth[at+1:]
		if hostport == "" {
			return false, false
		}
	}
	// The host runs to a ":" outside brackets.
	host, port, hasPort := hostport, "", false
	inside := false
	for i := 0; i < len(hostport); i++ {
		switch hostport[i] {
		case '[':
			inside = true
		case ']':
			inside = false
		case ':':
			if !inside {
				host, port, hasPort = hostport[:i], hostport[i+1:], true
				i = len(hostport)
			}
		}
	}
	if host == "" {
		return false, false
	}
	if hasPort {
		v := 0
		for i := 0; i < len(port); i++ {
			if !isDigit(port[i]) {
				return false, false
			}
			v = v*10 + int(port[i]-'0')
			if v > 65535 {
				return false, false
			}
		}
	}
	return hostOK(host)
}

func isAlpha(c byte) bool { return c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' }
func isDigit(c byte) bool { return c >= '0' && c <= '9' }
func isHex(c byte) bool   { return isDigit(c) || c >= 'a' && c <= 'f' || c >= 'A' && c <= 'F' }

func hexVal(c byte) int {
	switch {
	case isDigit(c):
		return int(c - '0')
	case c >= 'a' && c <= 'f':
		return int(c-'a') + 10
	default:
		return int(c-'A') + 10
	}
}

// hostOK is the WHATWG host parser for a special scheme, deciding success.
func hostOK(input string) (ok, narrowed bool) {
	if strings.HasPrefix(input, "[") {
		if !strings.HasSuffix(input, "]") {
			return false, false
		}
		return ipv6OK(input[1 : len(input)-1]), false
	}
	// Percent-decode.
	var d []byte
	for i := 0; i < len(input); i++ {
		c := input[i]
		if c == '%' && i+2 < len(input) && isHex(input[i+1]) && isHex(input[i+2]) {
			d = append(d, byte(hexVal(input[i+1])<<4|hexVal(input[i+2])))
			i += 2
			continue
		}
		d = append(d, c)
	}
	for _, c := range d {
		if c >= 0x80 {
			return false, true
		}
	}
	domain := strings.ToLower(string(d))
	for _, label := range strings.Split(domain, ".") {
		if strings.HasPrefix(label, "xn--") {
			return false, true
		}
	}
	if domain == "" {
		return false, false
	}
	for i := 0; i < len(domain); i++ {
		if forbiddenDomain(domain[i]) {
			return false, false
		}
	}
	if endsInNumber(domain) {
		return ipv4OK(domain), false
	}
	return true, false
}

// forbiddenDomain is a forbidden domain code point that ASCII can hold.
func forbiddenDomain(c byte) bool {
	if c <= 0x1f || c == 0x7f || c == '%' {
		return true
	}
	return strings.IndexByte(" #/:<>?@[\\]^|", c) >= 0
}

// ipv4Number parses one part of an IPv4 address: hex after "0x", octal after
// a leading "0", else decimal. It caps the value so that a long part cannot
// overflow; any value past 2^32 fails the parse either way.
func ipv4Number(s string) (uint64, bool) {
	if s == "" {
		return 0, false
	}
	r := uint64(10)
	switch {
	case len(s) >= 2 && (s[:2] == "0x" || s[:2] == "0X"):
		s, r = s[2:], 16
	case len(s) >= 2 && s[0] == '0':
		s, r = s[1:], 8
	}
	if s == "" {
		return 0, true
	}
	var v uint64
	for i := 0; i < len(s); i++ {
		c := s[i]
		var dv uint64
		switch {
		case r == 10 && isDigit(c), r == 8 && c >= '0' && c <= '7':
			dv = uint64(c - '0')
		case r == 16 && isHex(c):
			dv = uint64(hexVal(c))
		default:
			return 0, false
		}
		if v < 1<<40 {
			v = v*r + dv
		}
	}
	return v, true
}

func endsInNumber(s string) bool {
	parts := strings.Split(s, ".")
	if parts[len(parts)-1] == "" {
		if len(parts) == 1 {
			return false
		}
		parts = parts[:len(parts)-1]
	}
	last := parts[len(parts)-1]
	if last != "" {
		all := true
		for i := 0; i < len(last); i++ {
			if !isDigit(last[i]) {
				all = false
				break
			}
		}
		if all {
			return true
		}
	}
	_, ok := ipv4Number(last)
	return ok
}

func ipv4OK(s string) bool {
	parts := strings.Split(s, ".")
	if parts[len(parts)-1] == "" && len(parts) > 1 {
		parts = parts[:len(parts)-1]
	}
	if len(parts) > 4 {
		return false
	}
	nums := make([]uint64, len(parts))
	for i, p := range parts {
		v, ok := ipv4Number(p)
		if !ok {
			return false
		}
		nums[i] = v
	}
	for _, v := range nums[:len(nums)-1] {
		if v > 255 {
			return false
		}
	}
	return nums[len(nums)-1] < uint64(1)<<(8*(5-len(nums)))
}

// ipv6OK is the WHATWG IPv6 parser, deciding success.
func ipv6OK(s string) bool {
	var addr [8]uint64
	piece, compress := 0, -1
	p := 0
	at := func(i int) int {
		if i < len(s) {
			return int(s[i])
		}
		return -1
	}
	if at(p) == ':' {
		if at(p+1) != ':' {
			return false
		}
		p += 2
		piece++
		compress = piece
	}
	for at(p) != -1 {
		if piece == 8 {
			return false
		}
		if at(p) == ':' {
			if compress != -1 {
				return false
			}
			p++
			piece++
			compress = piece
			continue
		}
		value, length := uint64(0), 0
		for length < 4 && at(p) != -1 && isHex(byte(at(p))) {
			value = value*16 + uint64(hexVal(byte(at(p))))
			p++
			length++
		}
		if at(p) == '.' {
			if length == 0 {
				return false
			}
			p -= length
			if piece > 6 {
				return false
			}
			seen := 0
			for at(p) != -1 {
				v := -1
				if seen > 0 {
					if at(p) == '.' && seen < 4 {
						p++
					} else {
						return false
					}
				}
				if at(p) == -1 || !isDigit(byte(at(p))) {
					return false
				}
				for at(p) != -1 && isDigit(byte(at(p))) {
					n := at(p) - '0'
					switch {
					case v == -1:
						v = n
					case v == 0:
						return false
					default:
						v = v*10 + n
					}
					if v > 255 {
						return false
					}
					p++
				}
				addr[piece] = addr[piece]*0x100 + uint64(v)
				seen++
				if seen == 2 || seen == 4 {
					piece++
				}
			}
			if seen != 4 {
				return false
			}
			break
		} else if at(p) == ':' {
			p++
			if at(p) == -1 {
				return false
			}
		} else if at(p) != -1 {
			return false
		}
		addr[piece] = value
		piece++
	}
	if compress == -1 && piece != 8 {
		return false
	}
	return true
}
