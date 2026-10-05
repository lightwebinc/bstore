package vectors

import (
	"bytes"
	"fmt"

	"github.com/lightwebinc/bstore/internal/storerec"
)

// decodeKind decodes raw as the record kind named, returning its view.
func decodeKind(kind string, raw []byte) (map[string]any, []byte, error) {
	switch kind {
	case "bucket":
		r, err := storerec.DecodeBucket(raw)
		if err != nil {
			return nil, nil, err
		}
		b, err := r.Encode()
		return r.View(), b, err
	case "entry":
		r, err := storerec.DecodeEntry(raw)
		if err != nil {
			return nil, nil, err
		}
		b, err := r.Encode()
		return r.View(), b, err
	case "piece":
		r, err := storerec.DecodePiece(raw)
		if err != nil {
			return nil, nil, err
		}
		b, err := r.Encode()
		return r.View(), b, err
	case "outcome":
		r, err := storerec.DecodeOutcome(raw)
		if err != nil {
			return nil, nil, err
		}
		b, err := r.Encode()
		return r.View(), b, err
	case "front":
		r, err := storerec.DecodeFront(raw)
		if err != nil {
			return nil, nil, err
		}
		b, err := r.Encode()
		return r.View(), b, err
	case "request":
		r, err := storerec.DecodeRequest(raw)
		if err != nil {
			return nil, nil, err
		}
		b, err := r.Encode()
		return r.View(), b, err
	case "body":
		r, err := storerec.DecodeBody(raw)
		if err != nil {
			return nil, nil, err
		}
		b, err := r.Encode()
		return r.View(), b, err
	}
	return nil, nil, fmt.Errorf("unknown kind %s", kind)
}

type hexCase struct {
	Kind string         `json:"kind"`
	Name string         `json:"name"`
	Hex  string         `json:"hex"`
	View map[string]any `json:"view"`
}

type encoder interface{ Encode() ([]byte, error) }

func genHex(files map[string][]byte) error {
	reqRaw, body, signed, _ := signedPass()
	cases := []struct {
		kind, name string
		r          encoder
		raw        []byte
	}{
		{"bucket", "first token, seq 0", sampleBucket0(), nil},
		{"bucket", "seq 1: a writer, two puts, one consumed", sampleBucket1(), nil},
		{"bucket", "seq 2: a removal naming its delete, a key from a later version", sampleBucket2(), nil},
		{"entry", "put with etag, meta and hold terms", samplePut(), nil},
		{"entry", "put with only the required keys", samplePutMinimal(), nil},
		{"entry", "the empty object", sampleEmpty(), nil},
		{"entry", "delete, a key from a later version", sampleDelete(), nil},
		{"entry", "kind 7, admitted as unknown, keys 7 to 15 kept unread", sampleUnknownKind(), nil},
		{"piece", "the root node of the sample closure", samplePiece(), nil},
		{"front", "a front for an advertiser", sampleFront(), nil},
		{"request", "a request answered by its provider", sampleRequest(), nil},
		{"request", "a request with range, answered by a front", sampleRequestRange(), nil},
		{"outcome", "pass", sampleOutcome(storerec.OutcomePass), nil},
		{"outcome", "invalid", sampleOutcome(storerec.OutcomeInvalid), nil},
		{"outcome", "no-answer", sampleOutcome(storerec.OutcomeNoAnswer), nil},
		{"outcome", "unretrievable", sampleOutcome(storerec.OutcomeUnretrievable), nil},
		{"body", "the sample response body, inline", nil, body},
	}
	var out []hexCase
	for _, c := range cases {
		raw := c.raw
		if raw == nil {
			var err error
			if raw, err = c.r.Encode(); err != nil {
				return fmt.Errorf("hex %s: %w", c.name, err)
			}
		}
		view, again, err := decodeKind(c.kind, raw)
		if err != nil {
			return fmt.Errorf("hex %s: %w", c.name, err)
		}
		if !bytes.Equal(again, raw) {
			return fmt.Errorf("hex %s: does not re-encode", c.name)
		}
		out = append(out, hexCase{Kind: c.kind, Name: c.name, Hex: hx(raw), View: view})
	}
	return put(files, "hex-v1.json", map[string]any{
		"note":           "each record's canonical encoding and its fields; a decoder reads the hex to the view and re-encodes it to the same bytes",
		"records":        out,
		"signedResponse": hx(signed),
		"request":        hx(reqRaw),
	})
}
