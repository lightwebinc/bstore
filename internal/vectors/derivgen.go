package vectors

import (
	"fmt"
	"strings"

	"github.com/bsv-blockchain/go-sdk/wallet"

	"github.com/lightwebinc/bcommon/pushdrop"

	"github.com/lightwebinc/bstore/internal/storerec"
)

func genDerivation(files map[string][]byte) error {
	reg := map[string]any{
		"protocol":       map[string]any{"securityLevel": 1, "protocol": storerec.ProtocolName},
		"keyIds":         storerec.KeyIDs,
		"tagPrefix":      hx(storerec.TagPrefix),
		"tagToken":       hx(storerec.TagToken),
		"tagFunding":     hx(storerec.TagFunding),
		"tagHold":        hx(storerec.TagHold),
		"tagReserved":    "737404",
		"neverAssigned":  []string{"00", "62", "65", "66", "6f", "70", "71", "72"},
		"magicBucket":    hx(storerec.MagicBucket),
		"magicEntry":     hx(storerec.MagicEntry),
		"magicPiece":     hx(storerec.MagicPiece),
		"magicOutcome":   hx(storerec.MagicOutcome),
		"magicFront":     hx(storerec.MagicFront),
		"magicRequest":   hx(storerec.MagicRequest),
		"magicResponse":  hx(storerec.MagicResponse),
		"recordsPrefix":  storerec.RecordsPrefix,
		"blobPrefix":     storerec.BlobPrefix,
		"lookupService":  storerec.LookupService,
		"hostTerms":      "<base>" + storerec.HostTerms,
		"providerRoutes": []string{"<front>/challenge", "<front>/renew", "<front>/settle", "<front>/terms"},
		"frontSuffix":    storerec.FrontSuffix,
		"baskets":        storerec.Baskets,
		"domainBucketId": storerec.DomainBucketID,
		"borrowed": []map[string]any{
			{"securityLevel": 2, "protocol": storerec.AdProtocolName, "keyId": storerec.AdKeyID, "use": "BRC-26 advertisement lock, as the SDK has it"},
			{"securityLevel": 2, "protocol": storerec.SettleProtocolName, "use": "BRC-29 settlement outputs"},
		},
		"anyoneKey": hx(anyonePK),
	}
	// Each test identity's derived keys, counterparty anyone, as a reader
	// derives them from the identity key alone; the wallet's own derivation
	// (forSelf) must agree.
	var ids []map[string]any
	for _, p := range Parties {
		keys := map[string]string{}
		for _, kid := range storerec.KeyIDs {
			d := pushdrop.Derivation{Protocol: storerec.Protocol, KeyID: kid}
			if kid == storerec.KeyFund {
				continue // counterparty self; no rule reads it
			}
			reader := must(d.ExpectedLockingKey(p.Priv.PubKey()))
			own := must(wallet.NewKeyDeriver(p.Priv).DerivePublicKey(storerec.Protocol, kid, pushdrop.Anyone(), true))
			if !reader.IsEqual(own) {
				return fmt.Errorf("derivation %s %s: reader and wallet disagree", p.Name, kid)
			}
			keys[kid] = hx(reader.Compressed())
		}
		ad := must(storerec.AdDerivation.ExpectedLockingKey(p.Priv.PubKey()))
		ids = append(ids, map[string]any{"name": p.Name, "priv": hx(p.Priv.Serialize()), "identity": hx(p.Pub()),
			"keys": keys, "advertisementLock": hx(ad.Compressed())})
	}
	// Bucket identifiers: valid ones with their topics and S3 names, and
	// every way to break the grammar.
	type idCase struct {
		ID      string `json:"id"`
		OK      bool   `json:"ok"`
		Records string `json:"records,omitempty"`
		Blob    string `json:"blob,omitempty"`
		S3      string `json:"s3,omitempty"`
		S3OK    bool   `json:"s3ok"`
	}
	var idents []idCase
	for _, s := range []string{Bucket, OtherBucket, "a_qzvmkwerty", strings.Repeat("z", 25) + "_qzvmkwerty", "a_b_c_qzvmkwerty",
		"sthree_qzvmkwerty", "sthreex_qzvmkwerty", "x_sthreeabcd",
		"", "a_qzvmkwert", "a_qzvmkwertyy", strings.Repeat("z", 26) + "_qzvmkwerty", "A_qzvmkwerty", "_a_qzvmkwerty",
		"a__b_qzvmkwerty", "a__qzvmkwerty", "a_b_qzvmkwerTy", "a-b_qzvmkwerty", "a1_qzvmkwerty", "qzvmkwerty",
		"a_qzvmkwer y", "a_qzvmkwerté"} {
		c := idCase{ID: s, OK: storerec.CheckBucket(s) == nil}
		if c.OK {
			c.Records, c.Blob = storerec.RecordsTopic(s), storerec.BlobTopic(s)
			c.S3, c.S3OK = storerec.S3Name(s)
		}
		idents = append(idents, c)
	}
	var bids []map[string]any
	for i, o := range []struct {
		txid [32]byte
		vout uint32
	}{{firstOutpoint, 0}, {firstOutpoint, 1}, {label("another tree"), 1022}, {[32]byte{}, 0xffffffff}} {
		bids = append(bids, map[string]any{"txid": h32(o.txid), "vout": o.vout, "id": h32(storerec.BucketID(o.txid, o.vout)), "index": i})
	}
	var fronts []map[string]any
	for _, u := range []string{
		"https://host.example/chirp/v1/X/objects/X",
		"https://host.example/prefix/chirp/v1/X/objects/X",
		"https://host.example/chirp/v1/a/chirp/v1/X/objects/X",
		"https://host.example/no-chirp-here",
	} {
		f, ok := storerec.DerivedFront(u)
		fronts = append(fronts, map[string]any{"url": u, "front": f, "ok": ok})
	}
	return put(files, "derivation-v1.json", map[string]any{
		"note":           "the registry values; each test identity's derived keys under [1, \"bstore\"] (counterparty anyone) and its BRC-26 advertisement lock; bucket identifiers, topics, S3 names and bucket ids; derived fronts",
		"registry":       reg,
		"identities":     ids,
		"identifiers":    idents,
		"bucketIds":      bids,
		"derivedFronts":  fronts,
		"chirpURLSuffix": map[string]string{"root": h32(contentBuilt.RootHash), "suffix": storerec.CHIRPURLSuffix(contentBuilt.RootHash)},
	})
}
