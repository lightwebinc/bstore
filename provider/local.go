package provider

import (
	"context"
	"errors"

	"github.com/lightwebinc/bstore/internal/publish"
	"github.com/lightwebinc/bstore/storerec"
)

// TakeLocal takes a piece carrier, or a record, from a gateway in this
// process: a storage node, the gateway's local_provider. It is the feeder
// path without the socket, and with the check of a piece optional; a record
// is always checked (takeLocalRecord).
//
// With verify set, or for a piece the gateway gives no account of (lp.Known
// unset: an item read back from its outbox after a restart), the carrier is
// checked as any submission is (storerec.CheckPieceCarrier) before it is
// taken. With verify unset and an account given, the gateway's account is
// taken as given: the record's bucket, root, writer and piece, the piece's
// SHA-256, the funding output and the carrier's txid are the ones it minted,
// and only the piece's segment leaves are hashed here. The bucket's writer
// list, the funding output's one carrier and the room are held as always,
// and the closure walk still checks the closure's structure and content
// hash before the root is advertised.
//
// The piece is kept by reference: lp.BEEF is this provider's from here on
// (the gateway does not reuse it). A refusal comes back as publish.Refused,
// which the gateway neither counts delivered nor sends again; a store error
// or an unmined funding tree comes back as an error, and the gateway sends
// the piece again later.
func (p *Provider) TakeLocal(ctx context.Context, lp publish.LocalPiece, verify bool) error {
	b, blob := p.bucketOf(lp.Topic)
	if b == nil {
		return publish.Refused("not a topic of this provider's")
	}
	if !blob {
		return p.takeLocalRecord(ctx, lp)
	}
	select {
	case p.limits.takes <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	}
	defer func() { <-p.limits.takes }()
	mode := "trusted"
	var pc *storerec.PieceCarrier
	var known *[32]byte
	if verify || !lp.Known {
		mode = "verified"
		var err error
		if pc, err = storerec.CheckPieceCarrier(ctx, lp.BEEF, b.Bucket, p.hc); err != nil {
			if errors.Is(err, storerec.ErrUnmined) {
				p.metrics.inc("bstore_provider_local_takes_total", "result=unmined", "mode="+mode)
				return err
			}
			p.metrics.inc("bstore_provider_local_takes_total", "result=invalid", "mode="+mode)
			return publish.Refused("piece: " + why(err))
		}
	} else {
		if lp.Bucket != b.Bucket {
			return publish.Refused("piece: not this topic's bucket")
		}
		pc = &storerec.PieceCarrier{Piece: &storerec.Piece{Bucket: lp.Bucket, Writer: lp.Writer, Root: lp.Root, Piece: lp.Piece},
			Commitment: lp.Commitment, Funding: lp.Funding}
		known = &lp.Hash
	}
	w := p.takePiece(b, pc, known)
	switch {
	case w == storeFailed:
		p.metrics.inc("bstore_provider_local_takes_total", "result=error", "mode="+mode)
		return errors.New(w)
	case w != "":
		p.metrics.inc("bstore_provider_local_takes_total", "result=refused", "mode="+mode)
		return publish.Refused(w)
	}
	p.metrics.inc("bstore_provider_local_takes_total", "result=taken", "mode="+mode)
	return nil
}

// takeLocalRecord takes a record (an entry, a token, an outcome) from the
// gateway in this process as the submit route takes one: checked always, a
// record being small, and an entry on disk before the gateway hears it was
// taken.
func (p *Provider) takeLocalRecord(ctx context.Context, lp publish.LocalPiece) error {
	_, entry, w := p.take(ctx, lp.Topic, lp.BEEF, nil)
	if w != "" {
		p.metrics.inc("bstore_provider_local_takes_total", "result=refused", "mode=record")
		return publish.Refused(w)
	}
	if entry != "" {
		if err := p.journalEntry(entry); err != nil {
			p.logf("entry %s: journal: %v", short(entry), err)
			p.metrics.inc("bstore_provider_local_takes_total", "result=error", "mode=record")
			return err
		}
	}
	p.metrics.inc("bstore_provider_local_takes_total", "result=taken", "mode=record")
	return nil
}
