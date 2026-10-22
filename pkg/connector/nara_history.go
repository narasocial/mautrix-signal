// mautrix-signal - A Matrix-Signal puppeting bridge.
// Copyright (C) 2026 narasocial
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <https://www.gnu.org/licenses/>.

package connector

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/rs/zerolog"
	"go.mau.fi/util/jsontime"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/simplevent"
	"maunium.net/go/mautrix/bridgev2/status"

	"go.mau.fi/mautrix-signal/pkg/signalid"
)

// How a login's history is going, in its bridge states' info (for Nara, https://nara.contact):
//
//	history           "started" from the link until every chat in the phone's history transfer has its
//	                  portal and backfill, then "done"
//	history_transfer  "received" once the transfer is in, "none" when the phone sent none (the human
//	                  chose not to transfer), the phone's reason for refusing it, lowercased
//	                  ("continue_without_upload", "relink_requested"), or "failed" when it never
//	                  came (an hour's wait) or couldn't be read, with history "done". The bridge
//	                  tries again on its next start: if that works, history is "started" again,
//	                  with "received", until its chats are synced
const (
	NaraHistoryStarted = "started"
	NaraHistoryDone    = "done"

	NaraTransferReceived = "received"
	NaraTransferNone     = "none"
	NaraTransferFailed   = "failed"
)

// naraResyncWait is the longest syncChats waits for the portals it queued chat resyncs for. A resync
// whose portal fails to be created never reports back.
const naraResyncWait = time.Hour

var _ status.BridgeStateFiller = (*SignalClient)(nil)

func (s *SignalClient) FillBridgeState(state status.BridgeState) status.BridgeState {
	meta, ok := s.UserLogin.Metadata.(*signalid.UserLoginMetadata)
	if !ok {
		return state
	}
	return fillNaraHistory(state, meta)
}

func fillNaraHistory(state status.BridgeState, meta *signalid.UserLoginMetadata) status.BridgeState {
	if state.Info == nil {
		state.Info = make(map[string]any)
	}
	// Logins from before Nara's fork have none, and are done.
	if meta.NaraHistory == NaraHistoryStarted {
		state.Info["history"] = NaraHistoryStarted
	} else {
		state.Info["history"] = NaraHistoryDone
	}
	if meta.NaraHistoryTransfer != "" {
		state.Info["history_transfer"] = meta.NaraHistoryTransfer
	}
	return state
}

// naraLoginMetadata is a new login's metadata: history has started if the phone is transferring it.
func naraLoginMetadata(transferring bool) *signalid.UserLoginMetadata {
	if transferring {
		return &signalid.UserLoginMetadata{NaraHistory: NaraHistoryStarted}
	}
	return &signalid.UserLoginMetadata{NaraHistory: NaraHistoryDone, NaraHistoryTransfer: NaraTransferNone}
}

// naraTransferFailed reports history done, without it, when the transfer never came or couldn't be
// read, so the status endpoint isn't left waiting on "started". Nothing is reported when the sync was
// cancelled (a disconnect or logout), which isn't a failure.
func (s *SignalClient) naraTransferFailed(ctx context.Context) {
	if ctx.Err() != nil {
		return
	}
	markNaraTransferFailed(s.UserLogin.Metadata.(*signalid.UserLoginMetadata))
	if err := s.UserLogin.Save(ctx); err != nil {
		zerolog.Ctx(ctx).Err(err).Msg("Failed to save user login metadata after the transfer failed")
	}
	s.resendBridgeState()
}

// markNaraTransferReceived has history started until its chats are synced, also when an earlier try
// at the transfer had failed and reported it done.
func markNaraTransferReceived(meta *signalid.UserLoginMetadata) {
	meta.NaraHistory = NaraHistoryStarted
	meta.NaraHistoryTransfer = NaraTransferReceived
	meta.NaraTransferReceivedAt = jsontime.UnixNow()
}

func markNaraTransferFailed(meta *signalid.UserLoginMetadata) {
	meta.NaraHistory = NaraHistoryDone
	meta.NaraHistoryTransfer = NaraTransferFailed
}

func naraTransferRefused(reason string) string {
	return strings.ToLower(reason)
}

// resendBridgeState sends the login's latest bridge state again, to report a change in what
// FillBridgeState adds to it without waiting for the connection state to change.
func (s *SignalClient) resendBridgeState() {
	prev := s.UserLogin.BridgeState.GetPrevUnsent()
	if prev.StateEvent == "" {
		return
	}
	s.UserLogin.BridgeState.Send(status.BridgeState{
		StateEvent: prev.StateEvent,
		Error:      prev.Error,
		Message:    prev.Message,
		UserAction: prev.UserAction,
	})
}

// naraResyncs counts the chat resyncs syncChats queues, so history is only done once every chat's
// portal has handled its resync (creating the room and backfilling it).
type naraResyncs struct {
	wg sync.WaitGroup
}

// track makes the resync count itself done once its portal has handled it, and returns a func that
// counts it done now, for a resync that was handled on the spot or refused. Either can be called
// any number of times.
func (r *naraResyncs) track(evt *simplevent.ChatResync) (done func()) {
	var once sync.Once
	r.wg.Add(1)
	done = func() { once.Do(r.wg.Done) }
	prev := evt.PostHandleFunc
	evt.PostHandleFunc = func(ctx context.Context, portal *bridgev2.Portal) {
		if prev != nil {
			prev(ctx, portal)
		}
		done()
	}
	return done
}

// wait waits for every tracked resync, until ctx is done or maxWait has passed. It reports whether
// they all were.
func (r *naraResyncs) wait(ctx context.Context, maxWait time.Duration) bool {
	all := make(chan struct{})
	go func() {
		r.wg.Wait()
		close(all)
	}()
	timer := time.NewTimer(maxWait)
	defer timer.Stop()
	select {
	case <-all:
		return true
	case <-timer.C:
		return false
	case <-ctx.Done():
		return false
	}
}
