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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/simplevent"
	"maunium.net/go/mautrix/bridgev2/status"

	"go.mau.fi/mautrix-signal/pkg/signalid"
)

func TestFillNaraHistory(t *testing.T) {
	cases := []struct {
		name     string
		meta     signalid.UserLoginMetadata
		history  string
		transfer any
	}{
		{"transferring", *naraLoginMetadata(true), "started", nil},
		{"no transfer", *naraLoginMetadata(false), "done", "none"},
		{"received and synced", signalid.UserLoginMetadata{NaraHistory: "done", NaraHistoryTransfer: "received"}, "done", "received"},
		{"refused", signalid.UserLoginMetadata{NaraHistory: "done", NaraHistoryTransfer: naraTransferRefused("CONTINUE_WITHOUT_UPLOAD")}, "done", "continue_without_upload"},
		{"login from before the fork", signalid.UserLoginMetadata{ChatsSynced: true}, "done", nil},
		{"never came", func() signalid.UserLoginMetadata { m := naraLoginMetadata(true); markNaraTransferFailed(m); return *m }(), "done", "failed"},
		{"came on a later try", func() signalid.UserLoginMetadata {
			m := naraLoginMetadata(true)
			markNaraTransferFailed(m)
			markNaraTransferReceived(m)
			return *m
		}(), "started", "received"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			state := fillNaraHistory(status.BridgeState{StateEvent: status.StateConnected}, &c.meta)
			assert.Equal(t, status.StateConnected, state.StateEvent)
			assert.Equal(t, c.history, state.Info["history"])
			assert.Equal(t, c.transfer, state.Info["history_transfer"])
		})
	}
}

func TestFillNaraHistoryKeepsOtherInfo(t *testing.T) {
	state := fillNaraHistory(status.BridgeState{Info: map[string]any{"other": 1}}, naraLoginMetadata(true))
	assert.Equal(t, 1, state.Info["other"])
	assert.Equal(t, "started", state.Info["history"])
}

func TestNaraResyncsWaitsForEveryPortal(t *testing.T) {
	var resyncs naraResyncs
	var prevCalled bool
	first := &simplevent.ChatResync{EventMeta: simplevent.EventMeta{PostHandleFunc: func(context.Context, *bridgev2.Portal) { prevCalled = true }}}
	second := &simplevent.ChatResync{}
	resyncs.track(first)
	resyncs.track(second)

	assert.False(t, resyncs.wait(context.Background(), 20*time.Millisecond), "nothing handled yet")
	first.PostHandle(context.Background(), nil)
	assert.True(t, prevCalled, "the resync's own PostHandleFunc still runs")
	assert.False(t, resyncs.wait(context.Background(), 20*time.Millisecond), "one left")
	second.PostHandle(context.Background(), nil)
	assert.True(t, resyncs.wait(context.Background(), time.Second))
}

func TestNaraResyncsDoneTwiceCountsOnce(t *testing.T) {
	var resyncs naraResyncs
	handled := &simplevent.ChatResync{}
	waiting := &simplevent.ChatResync{}
	done := resyncs.track(handled)
	resyncs.track(waiting)

	// Handled on the spot: PostHandle has run, and syncChats calls done too.
	handled.PostHandle(context.Background(), nil)
	done()
	done()
	assert.False(t, resyncs.wait(context.Background(), 20*time.Millisecond), "the other one is still waiting")
	waiting.PostHandle(context.Background(), nil)
	assert.True(t, resyncs.wait(context.Background(), time.Second))
}

func TestNaraResyncsWaitStopsWithContext(t *testing.T) {
	var resyncs naraResyncs
	resyncs.track(&simplevent.ChatResync{})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	assert.False(t, resyncs.wait(ctx, time.Hour))
}

func TestNaraResyncsNothingTracked(t *testing.T) {
	var resyncs naraResyncs
	assert.True(t, resyncs.wait(context.Background(), time.Second))
}
