// Package engineask keeps a host's overlay engine carrying the topics an
// application needs: it asks the engine on the same host for them (POST
// /admin/topics, an engine route that answers loopback callers only) and asks
// again before the ask lapses.
//
// An ask has an id, a lifetime and the list items it appends to the engine's
// settings. The engine carries the items while the ask lives, reloads only
// the modules they change, and its overlay bridge then asks the delivery
// edge for the topics. Asking again with the same id replaces the ask, so an
// application that restarts with other buckets replaces what it asked for
// before, at once.
//
// An Asker does not withdraw its ask when it stops. A restart, the common
// case, asks again within the lifetime, so the topics never stop and the
// edge never withdraws and grants them again; an application that stays
// stopped has its ask lapse after the lifetime. Nothing is kept on disk.
package engineask

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// DefaultTTL is an ask's lifetime when none is set. Engines bound it
// (OVERLAY_TOPICS_DYNAMIC_MAX_TTL_SECONDS; a week by default, ten minutes on
// the lab hosts), so it is kept short.
const DefaultTTL = 10 * time.Minute

// Asker keeps one ask alive. Set the fields, then call Run.
type Asker struct {
	// URL is the engine's base URL, such as http://127.0.0.1:8280.
	URL string
	// ID names the ask; asking again with it replaces the ask.
	ID string
	// TTL is the ask's lifetime; it is asked again at half of it. Zero is
	// DefaultTTL.
	TTL time.Duration
	// Settings are the list items asked for, by setting: OVERLAY_TOPICS
	// and the lists of the module that carries them.
	Settings map[string][]string
	// Client defaults to a client with a 10 s timeout.
	Client *http.Client
	// Logf reports a change of outcome: the first success, and each
	// refusal or failure that differs from the last. Nil logs nothing.
	Logf func(format string, args ...any)
	// OnResult, when set, is told the outcome of every ask.
	OnResult func(ok bool)

	last string // the last outcome logged
}

// Check reports whether the asker is usable.
func (a *Asker) Check() error {
	u, err := url.Parse(a.URL)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return fmt.Errorf("engine URL %q is not an http URL", a.URL)
	}
	if a.ID == "" || strings.ContainsAny(a.ID, "/?#") {
		return fmt.Errorf("ask id %q is empty or not one path segment", a.ID)
	}
	if len(a.Settings) == 0 {
		return fmt.Errorf("ask %s names no settings", a.ID)
	}
	if a.TTL != 0 && a.TTL < time.Second {
		return fmt.Errorf("ask %s: a lifetime under a second is not one the engine takes", a.ID)
	}
	return nil
}

func (a *Asker) ttl() time.Duration {
	if a.TTL <= 0 {
		return DefaultTTL
	}
	return a.TTL
}

// Refused is an engine's refusal of the ask (an answer of 4xx): the engine
// does not allow a setting, an item, the lifetime, or one more ask. Asking
// again changes nothing until the engine's or the application's settings
// do.
type Refused struct {
	Status int
	Reason string
}

func (e *Refused) Error() string {
	if e.Reason == "" {
		return fmt.Sprintf("the engine refused the ask (%d)", e.Status)
	}
	return fmt.Sprintf("the engine refused the ask (%d): %s", e.Status, e.Reason)
}

// Run asks at once and again at half the lifetime, until ctx is done. A
// failed ask is tried again sooner, from 1 s doubling to the refresh
// interval, so an engine that was restarting is asked as soon as it answers.
// A refused ask is tried again at the refresh interval, and reported each
// time: nothing it asks for is carried until it is fixed.
func (a *Asker) Run(ctx context.Context) {
	every := a.ttl() / 2
	retry := time.Second
	for {
		wait := every
		if err := a.Ask(ctx); err != nil {
			if ctx.Err() != nil {
				return
			}
			var ref *Refused
			if !errors.As(err, &ref) {
				wait, retry = min(retry, every), min(retry*2, every)
			}
		} else {
			retry = time.Second
		}
		t := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			t.Stop()
			return
		case <-t.C:
		}
	}
}

// Ask sends the ask once.
func (a *Asker) Ask(ctx context.Context) error {
	err := a.ask(ctx)
	if a.OnResult != nil {
		a.OnResult(err == nil)
	}
	outcome := "ok"
	if err != nil {
		outcome = err.Error()
	}
	var ref *Refused
	loud := errors.As(err, &ref)
	if (outcome != a.last || loud) && a.Logf != nil && ctx.Err() == nil {
		if err == nil {
			a.Logf("the engine carries ask %s (%s, lifetime %s)", a.ID, a.summary(), a.ttl())
		} else {
			if loud {
				a.Logf("ENGINE REFUSED ask %s: %v; none of its items are carried; asking again in %s", a.ID, err, a.ttl()/2)
			} else {
				a.Logf("asking the engine for %s: %v; asking again", a.ID, err)
			}
		}
	}
	a.last = outcome
	return err
}

func (a *Asker) summary() string {
	n := 0
	for _, v := range a.Settings {
		n += len(v)
	}
	return fmt.Sprintf("%d items in %d settings", n, len(a.Settings))
}

func (a *Asker) ask(ctx context.Context) error {
	body, err := json.Marshal(struct {
		ID       string              `json:"id"`
		TTL      int64               `json:"ttl_seconds"`
		Settings map[string][]string `json:"settings"`
	}{a.ID, int64(a.ttl() / time.Second), a.Settings})
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(a.URL, "/")+"/admin/topics", bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	c := a.Client
	if c == nil {
		c = &http.Client{Timeout: 10 * time.Second}
	}
	resp, err := c.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	msg, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	if resp.StatusCode == http.StatusOK {
		return nil
	}
	var e struct {
		Error string `json:"error"`
	}
	_ = json.Unmarshal(msg, &e)
	if resp.StatusCode >= 400 && resp.StatusCode < 500 {
		return &Refused{Status: resp.StatusCode, Reason: e.Error}
	}
	if e.Error != "" {
		return fmt.Errorf("the engine answered %d: %s", resp.StatusCode, e.Error)
	}
	return fmt.Errorf("the engine answered %d", resp.StatusCode)
}
