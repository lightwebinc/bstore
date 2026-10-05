package vectors

import (
	"errors"
	"fmt"
	"strings"

	ec "github.com/bsv-blockchain/go-sdk/primitives/ec"
	"github.com/bsv-blockchain/go-sdk/wallet"

	"github.com/lightwebinc/bcommon/pushdrop"

	"github.com/lightwebinc/bstore/internal/storerec"
)

// adPriv is a party's advertisement key: [2, "uhrp advertisement"] key id 1,
// counterparty anyone.
func adPriv(p Party) *ec.PrivateKey {
	return must(wallet.NewKeyDeriver(p.Priv).DerivePrivateKey(storerec.AdProtocol, storerec.AdKeyID, pushdrop.Anyone()))
}

// adScript is p's advertisement of root at url.
func adScript(p Party, root [32]byte, url string, expiry, size uint64) []byte {
	f := must(storerec.AdvertisementFields(p.Pub(), adPriv(p), root, url, expiry, size))
	return storerec.AdvertisementScript(adPriv(p).PubKey(), f)
}

func adURL(root [32]byte) string { return "https://bob.example" + storerec.CHIRPURLSuffix(root) }

type scriptCase struct {
	Name   string `json:"name"`
	Script string `json:"script"`
	Claim  string `json:"claim"`
	// Advertisement: "ok" or "advertisement" with its rule.
	Ad     string `json:"advertisement"`
	AdRule string `json:"adRule,omitempty"`
	AdURL  string `json:"adUrl,omitempty"`
}

func genScript(files map[string][]byte) error {
	var cases []scriptCase
	add := func(name string, s []byte) {
		c := scriptCase{Name: name, Script: hx(s), Claim: string(storerec.Classify(s))}
		ad, err := storerec.DecodeAdvertisement(s)
		c.Ad = reason(err)
		if err == nil {
			c.Ad, c.AdURL = "ok", ad.URL
		} else {
			c.AdRule = storerec.Rule(err)
			if errors.Is(err, storerec.ErrNarrowed) {
				c.AdRule += "/narrowed"
			}
		}
		cases = append(cases, c)
	}
	key := storerec.EntryDerivation
	ak := must(key.ExpectedLockingKey(Alice.Priv.PubKey()))
	sig := make([]byte, 70)
	sig[0] = 0x30
	rec := must(samplePut().Encode())
	out := must(sampleOutcome(storerec.OutcomeNoAnswer).Encode())
	fr := must(sampleFront().Encode())
	pc := must(samplePiece().Encode())
	tok := must(sampleBucket1().Encode())
	pd := func(fields ...[]byte) []byte { return pushdrop.Script(ak, fields, sig) }
	// The classifier.
	add("a bucket token", pd(storerec.TagToken, tok))
	add("an entry", pd(rec))
	add("an outcome", pd(out))
	add("a front statement", pd(fr))
	add("a piece", pd(pc))
	add("an entry unsigned (it still claims)", pushdrop.Script(ak, [][]byte{rec}, nil))
	add("funding-shaped", storerec.FundingScript(ak))
	add("hold-shaped", storerec.HoldScript(ak))
	add("the reserved tag 73 74 04: nothing", pushdrop.Script(ak, [][]byte{{0x73, 0x74, 0x04}}, nil))
	add("funding tag signed: not funding-shaped", pushdrop.Script(ak, [][]byte{storerec.TagFunding}, sig))
	add("hold tag with OP_2DROP: not hold-shaped", append(storerec.HoldScript(ak)[:39], 0x6d))
	hp := storerec.HoldScript(ak)
	add("hold tag by OP_PUSHDATA1: not hold-shaped", append(append(append([]byte{}, hp[:35]...), 0x4c, 3), hp[36:]...))
	add("the token tag by OP_PUSHDATA1: claims, the lock later refuses", append(append(append([]byte{}, pd(storerec.TagToken, tok)[:35]...), 0x4c, 3), pd(storerec.TagToken, tok)[36:]...))
	add("an entry record in a 1-byte map head (24 keys or more)", pd(append([]byte{0xb8, 0x18}, rec[1:]...)))
	add("another record magic ste 02", pd(append(append([]byte{}, rec[:3]...), append([]byte{0x73, 0x74, 0x65, 0x02}, rec[7:]...)...)))
	add("a bucket record without the tag: nothing", pd(tok))
	add("magic bytes without a map head: nothing", pd([]byte{0x00, 0x44, 0x73, 0x74, 0x65, 0x01}))
	add("a 65-byte key: nothing", append(append([]byte{0x41}, make([]byte, 65)...), 0xac, 0x03, 0x73, 0x74, 0x02, 0x75))
	add("P2PKH: nothing", append(append([]byte{0x76, 0xa9, 0x14}, make([]byte, 20)...), 0x88, 0xac))
	add("empty: nothing", []byte{})
	add("the key and OP_CHECKSIG alone: nothing", append(append([]byte{0x21}, ak.Compressed()...), 0xac))

	// Advertisements, as the SDK decodes and verifies them.
	root := contentBuilt.RootHash
	good := func() [][]byte {
		return must(storerec.AdvertisementFields(Bob.Pub(), adPriv(Bob), root, adURL(root), 1900000000, uint64(len(contentBuilt.Root))))
	}
	lock := adPriv(Bob).PubKey()
	adOf := func(f [][]byte) []byte { return storerec.AdvertisementScript(lock, f) }
	resign := func(f [][]byte) [][]byte {
		var signed []byte
		for _, x := range f[:5] {
			signed = append(signed, x...)
		}
		d := sha256Sum(signed)
		f[5] = must(adPriv(Bob).Sign(d[:])).Serialize()
		return f
	}
	add("advertisement: Bob's, valid", adOf(good()))
	add("advertisement: the anyone key's (the SDK admits it; bstore never counts it)", adScript(Party{Name: "anyone", Priv: anyonePriv()}, root, adURL(root), 1900000000, 100))
	{
		f := good()
		f[5] = highS(f[5])
		add("advertisement: a high-S signature (the SDK takes any S)", adOf(f))
	}
	{
		f := good()
		add("advertisement: five fields", storerec.AdvertisementScript(lock, f[:5]))
		add("advertisement: seven fields", storerec.AdvertisementScript(lock, append(f, []byte{1})))
	}
	{
		s := adOf(good())
		// The URL field (a direct push) rewritten with OP_PUSHDATA1.
		f := good()
		head := append(append([]byte{0x21}, lock.Compressed()...), 0xac)
		var b []byte
		b = append(b, head...)
		for i, x := range f {
			if i == 1 {
				b = append(append(b, 0x4c, byte(len(x))), x...)
				continue
			}
			b = append(b, minimal(x)...)
		}
		b = append(b, 0x6d, 0x6d, 0x6d)
		add("advertisement: the hash by OP_PUSHDATA1, not minimal", b)
		add("advertisement: OP_DROP x6 where OP_2DROP x3 is canonical", append(s[:len(s)-3], 0x75, 0x75, 0x75, 0x75, 0x75, 0x75))
		add("advertisement: locked to Bob's identity key", storerec.AdvertisementScript(Bob.Priv.PubKey(), good()))
		add("advertisement: locked to another host's advertisement key", storerec.AdvertisementScript(adPriv(Carol).PubKey(), good()))
	}
	mutateAd := func(name string, i int, v []byte, sign bool) {
		f := good()
		f[i] = v
		if sign {
			f = resign(f)
		}
		add("advertisement: "+name, adOf(f))
	}
	mutateAd("a host key of 32 bytes", 0, Bob.Pub()[:32], true)
	mutateAd("a host key with prefix 04", 0, key04, true)
	mutateAd("a host key at or above the prime", 0, keyAlias, true)
	mutateAd("Carol as host, Bob's lock and signature", 0, Carol.Pub(), true)
	mutateAd("a hash of 31 bytes", 1, root[:31], true)
	mutateAd("a hash of 33 bytes", 1, append(root[:], 0), true)
	mutateAd("a url of 2,048 bytes", 2, []byte("https://a.example/"+strings.Repeat("x", 2048-18)), true)
	mutateAd("a url of 2,049 bytes", 2, []byte("https://a.example/"+strings.Repeat("x", 2049-18)), true)
	mutateAd("a url not UTF-8", 2, []byte("https://a.example/\xff"), true)
	mutateAd("a url with a BOM before it (the decoder drops one)", 2, append([]byte{0xef, 0xbb, 0xbf}, []byte(adURL(root))...), true)
	mutateAd("a url with two BOMs", 2, append([]byte{0xef, 0xbb, 0xbf, 0xef, 0xbb, 0xbf}, []byte(adURL(root))...), true)
	mutateAd("a url with a tab", 2, []byte("https://a.example/a\tb"), true)
	mutateAd("a url with U+0085", 2, []byte("https://a.example/a\u0085b"), true)
	mutateAd("an http url", 2, []byte("http://a.example/x"), true)
	mutateAd("an expiry of 0", 3, []byte{0}, true)
	mutateAd("an expiry not minimal", 3, []byte{0xfd, 0x10, 0x00}, true)
	mutateAd("an expiry with a trailing byte", 3, append(storerec.CompactSize(1900000000), 0), true)
	mutateAd("an expiry of 2^53 - 1", 3, storerec.CompactSize(storerec.MaxSafe), true)
	mutateAd("an expiry of 2^53", 3, storerec.CompactSize(storerec.MaxSafe+1), true)
	mutateAd("an expiry of 0xfd (three bytes, minimal)", 3, storerec.CompactSize(0xfd), true)
	mutateAd("an expiry empty (OP_0 reads as one zero byte)", 3, []byte{}, true)
	mutateAd("a file size of 0", 4, []byte{0}, true)
	mutateAd("a file size of 2^32 - 1 in nine bytes, not minimal", 4, []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0, 0, 0, 0}, true)
	mutateAd("a file size of 2^32 in nine bytes, minimal", 4, []byte{0xff, 0, 0, 0, 0, 1, 0, 0, 0}, true)
	mutateAd("a file size of 2^32 in five bytes (truncated)", 4, []byte{0xfe, 0, 0, 0, 0}, true)
	{
		f := good()
		f[5] = padR(f[5])
		add("advertisement: a signature with a zero byte before R", adOf(f))
		f = good()
		f[5] = f[5][:7]
		add("advertisement: a signature of 7 bytes", adOf(f))
		f = good()
		f[5] = must(Carol.Priv.Sign(make([]byte, 32))).Serialize()
		add("advertisement: a signature by another key", adOf(f))
		f = good()
		f[3] = storerec.CompactSize(1900000001)
		add("advertisement: an expiry changed after signing", adOf(f))
	}
	// The url as the WHATWG parser reads it, through the whole check.
	for _, u := range urlCorpus() {
		f := good()
		f[2] = []byte(u)
		add(fmt.Sprintf("advertisement url %q", u), adOf(resign(f)))
	}
	// Eligibility of an advertisement for the sample put's bounty.
	e := samplePut()
	minExp := e.Created/1000 + e.Hold.Retention
	rl := uint64(len(contentBuilt.Root))
	var elig []map[string]any
	addE := func(name string, p Party, rt [32]byte, url string, expiry, size uint64, entry *storerec.Entry) {
		s := adScript(p, rt, url, expiry, size)
		ad := must(storerec.DecodeAdvertisement(s))
		elig = append(elig, map[string]any{"name": name, "script": hx(s), "entry": hx(must(entry.Encode())), "rootLength": rl,
			"verdict": storerec.Eligibility(ad, entry, rl)})
	}
	addE("eligible", Bob, root, adURL(root), minExp, rl, e)
	addE("expiry one second short", Bob, root, adURL(root), minExp-1, rl, e)
	addE("another root", Bob, label("other root"), adURL(root), minExp, rl, e)
	addE("the anyone key", Party{Name: "anyone", Priv: anyonePriv()}, root, adURL(root), minExp, rl, e)
	addE("file size not the root node's", Bob, root, adURL(root), minExp, rl+1, e)
	addE("a url not of the CHIRP form", Bob, root, "https://bob.example/files/x", minExp, rl, e)
	addE("the CHIRP form in the query", Bob, root, "https://bob.example/?x="+storerec.CHIRPURLSuffix(root), minExp, rl, e)
	noHold := samplePut()
	noHold.Hold = nil
	addE("an entry without hold terms", Bob, root, adURL(root), minExp, rl, noHold)
	return put(files, "script-v1.json", map[string]any{
		"eligibility": elig,
		"note":        "the classifier (claim: bucket-token, entry, outcome, front, piece, funding, hold, or empty for nothing) and the BRC-26 advertisement check as the SDK makes it (advertisement: ok, or refused with the rule that failed); a url rule marked /narrowed is an internationalised host this Go codec refuses and the WHATWG parser may admit",
		"cases":       cases,
	})
}

func minimal(d []byte) []byte {
	s := storerec.AdvertisementScript(must(ec.PublicKeyFromBytes(Bob.Pub())), [][]byte{d})
	return s[35 : len(s)-1]
}

func padR(der []byte) []byte {
	rl := int(der[3])
	o := append([]byte{0x30, der[1] + 1, 0x02, der[3] + 1, 0x00}, der[4:4+rl]...)
	return append(o, der[4+rl:]...)
}

func anyonePriv() *ec.PrivateKey {
	k, _ := ec.PrivateKeyFromBytes(append(make([]byte, 31), 1))
	return k
}

// urlCorpus are urls built from parts that reach every branch of the
// WHATWG parse the SDK runs: schemes, slashes, user information, hosts
// (names, IPv4 forms, IPv6, percent-encoding, forbidden code points,
// internationalised names), ports, paths, queries and fragments, and
// surrounding spaces. Deterministic: a fixed linear congruential sequence
// picks the combinations.
func urlCorpus() []string {
	schemes := []string{"https", "https", "https", "https", "HTTPS", "hTtPs", "http", "ftp", "https+x", "1https", "ht tps"}
	seps := []string{"://", "://", "://", "://", ":", ":/", ":\\\\", ":///", "://\\", ":/\\/"}
	users := []string{"", "", "", "", "", "@", ":@", "u@", "u:@", ":p@", "u:p@", "@@", "a@b@", "%40@"}
	hosts := []string{"a.example", "A.EXAMPLE", "a..b", ".", "..", "a.", "1.2.3.4", "1.2.3.256", "1.2.3.4.5", "0x7f.1", "0x7F000001",
		"0x", "09", "08.1", "1.2.3", "4294967295", "4294967296", "1.2.3.4.", "1..2", "[::1]", "[1:2:3:4:5:6:7:8]", "[::ffff:1.2.3.4]",
		"[1::2::3]", "[::1", "[1:2:3:4:5:6:7:8:9]", "[::1.2.3]", "[::01.2.3.4]", "[1:2:3:4:5:6:1.2.3.4]", "[g::1]", "a%2eb", "%41.example",
		"%zz", "a b", "a<b", "a^b", "a|b", "a%00b", "a_b", "a~b", "a!b", "a*b", "xn--abc", "XN--ab", "a.xn--", "bücher.example",
		"%C3%BC.example", "١.example", "", "a%", "a]b", "localhost", "a.example。com"}
	ports := []string{"", "", "", "", ":", ":443", ":0", ":65535", ":65536", ":1a", ":-1", ":00080", ":99999999999999999999"}
	tails := []string{"", "/", "/chirp/v1/x/objects/x", "/a b", "/é", "?q", "#", "#f", "/#", "\\x", "/?#", "/a#b#c", "?#", "#é"}
	pads := []string{"", " ", "  "}
	var out []string
	seen := map[string]bool{}
	addU := func(u string) {
		if !seen[u] {
			seen[u] = true
			out = append(out, u)
		}
	}
	// Every host alone, then sampled combinations.
	for _, h := range hosts {
		addU("https://" + h + "/x")
	}
	x := uint64(0x2545f4914f6cdd1d)
	next := func(n int) int {
		x = x*6364136223846793005 + 1442695040888963407
		return int((x >> 33) % uint64(n))
	}
	for len(out) < 1500 {
		pad := pads[next(len(pads))]
		addU(pad + schemes[next(len(schemes))] + seps[next(len(seps))] + users[next(len(users))] + hosts[next(len(hosts))] +
			ports[next(len(ports))] + tails[next(len(tails))] + pads[next(len(pads))])
	}
	return out
}
