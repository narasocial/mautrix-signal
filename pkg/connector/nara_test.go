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
	"encoding/json"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/event"

	"go.mau.fi/mautrix-signal/pkg/libsignalgo"
	"go.mau.fi/mautrix-signal/pkg/signalid"
	"go.mau.fi/mautrix-signal/pkg/signalmeow/store"
	"go.mau.fi/mautrix-signal/pkg/signalmeow/types"
)

var (
	naraOwn   = uuid.MustParse("11111111-1111-4111-8111-111111111111")
	naraPeer  = uuid.MustParse("22222222-2222-4222-8222-222222222222")
	naraOther = uuid.MustParse("33333333-3333-4333-8333-333333333333")
	naraPNI   = uuid.MustParse("44444444-4444-4444-8444-444444444444")
	naraGroup = networkid.PortalID("YWJjZGVmZ2hpamtsbW5vcHFyc3R1dnd4eXphYmNkZWY=") // 44 characters
)

type fakeRecipients struct {
	byACI map[uuid.UUID]*types.Recipient
	byPNI map[uuid.UUID]*types.Recipient
	err   error
	calls int
}

func (f *fakeRecipients) LoadAndUpdateRecipient(_ context.Context, aci, pni uuid.UUID, _ store.RecipientUpdaterFunc) (*types.Recipient, error) {
	f.calls++
	if f.err != nil {
		return nil, f.err
	}
	if r, ok := f.byACI[aci]; ok && aci != uuid.Nil {
		return r, nil
	}
	if r, ok := f.byPNI[pni]; ok && pni != uuid.Nil {
		return r, nil
	}
	// The real store makes an empty row for someone it doesn't know.
	return &types.Recipient{ACI: aci, PNI: pni}, nil
}

func naraFixture() *fakeRecipients {
	return &fakeRecipients{
		byACI: map[uuid.UUID]*types.Recipient{
			naraOwn:   {ACI: naraOwn, E164: "+15550000001", Profile: types.Profile{Name: "Me"}},
			naraPeer:  {ACI: naraPeer, E164: "+447700900123", ContactName: "Sam Rivera", Nickname: "Sammy R", Profile: types.Profile{Name: "sam"}},
			naraOther: {ACI: naraOther, ContactName: "Alex", Profile: types.Profile{Name: "alex!"}},
		},
		byPNI: map[uuid.UUID]*types.Recipient{
			naraPNI: {PNI: naraPNI, E164: "4915123456789"},
		},
	}
}

func TestBuildNaraInfoDM(t *testing.T) {
	people := newNaraPeople(naraFixture())
	portalID := signalid.MakeDMPortalID(libsignalgo.NewACIServiceID(naraPeer))

	info, err := buildNaraInfo(context.Background(), people, naraOwn, portalID, "Sam Rivera", naraMessage{Sender: naraPeer, Timestamp: 1760000000123})
	require.NoError(t, err)
	assert.Equal(t, &NaraMessageInfo{
		V:                 1,
		Receiver:          naraOwn.String(),
		Chat:              naraPeer.String(),
		Direct:            true,
		PeerPhone:         "+447700900123",
		PeerName:          "Sammy R",
		PeerProfileName:   "sam",
		MessageID:         naraPeer.String() + "|1760000000123",
		Sender:            naraPeer.String(),
		SenderPhone:       "+447700900123",
		SenderName:        "Sammy R",
		SenderProfileName: "sam",
	}, info)
}

func TestBuildNaraInfoOwnMessageHasNoNames(t *testing.T) {
	people := newNaraPeople(naraFixture())
	portalID := signalid.MakeDMPortalID(libsignalgo.NewACIServiceID(naraOther))

	info, err := buildNaraInfo(context.Background(), people, naraOwn, portalID, "", naraMessage{Sender: naraOwn, Timestamp: 5, History: true})
	require.NoError(t, err)
	assert.True(t, info.FromMe)
	assert.True(t, info.History)
	assert.Equal(t, naraOwn.String(), info.Sender)
	assert.Equal(t, "+15550000001", info.SenderPhone)
	assert.Empty(t, info.SenderName)
	assert.Empty(t, info.SenderProfileName)
	// The peer is still named, and has no number the receiver knows.
	assert.Equal(t, "Alex", info.PeerName)
	assert.Equal(t, "alex!", info.PeerProfileName)
	assert.Empty(t, info.PeerPhone)
}

func TestBuildNaraInfoGroup(t *testing.T) {
	people := newNaraPeople(naraFixture())

	info, err := buildNaraInfo(context.Background(), people, naraOwn, naraGroup, "Climbing", naraMessage{Sender: naraOther, Timestamp: 7})
	require.NoError(t, err)
	assert.False(t, info.Direct)
	assert.Equal(t, string(naraGroup), info.Chat)
	assert.Equal(t, "Climbing", info.ChatName)
	assert.Empty(t, info.PeerName)
	assert.Equal(t, "Alex", info.SenderName)
	assert.Empty(t, info.SenderPhone)
}

func TestBuildNaraInfoNoteToSelf(t *testing.T) {
	people := newNaraPeople(naraFixture())
	portalID := signalid.MakeDMPortalID(libsignalgo.NewACIServiceID(naraOwn))

	info, err := buildNaraInfo(context.Background(), people, naraOwn, portalID, "Note to Self", naraMessage{Sender: naraOwn, Timestamp: 9})
	require.NoError(t, err)
	assert.True(t, info.Direct)
	assert.Equal(t, info.Receiver, info.Chat)
	assert.Empty(t, info.PeerName)
	assert.Empty(t, info.PeerPhone)
	assert.Empty(t, info.ChatName)
}

func TestBuildNaraInfoPNIChatAndPhoneWithoutPlus(t *testing.T) {
	people := newNaraPeople(naraFixture())
	portalID := signalid.MakeDMPortalID(libsignalgo.NewPNIServiceID(naraPNI))

	info, err := buildNaraInfo(context.Background(), people, naraOwn, portalID, "", naraMessage{Sender: naraOwn, Timestamp: 11})
	require.NoError(t, err)
	assert.Equal(t, "PNI:"+naraPNI.String(), info.Chat)
	assert.Equal(t, "+4915123456789", info.PeerPhone)
}

func TestBuildNaraInfoEdit(t *testing.T) {
	people := newNaraPeople(naraFixture())
	portalID := signalid.MakeDMPortalID(libsignalgo.NewACIServiceID(naraPeer))

	info, err := buildNaraInfo(context.Background(), people, naraOwn, portalID, "", naraMessage{Sender: naraPeer, Timestamp: 200, TargetStamp: 100})
	require.NoError(t, err)
	assert.Equal(t, naraPeer.String()+"|200", info.MessageID)
	assert.Equal(t, naraPeer.String()+"|100", info.TargetID)
}

func TestBuildNaraInfoStoreErrorStillHasIDs(t *testing.T) {
	fake := naraFixture()
	fake.err = errors.New("db down")
	people := newNaraPeople(fake)

	info, err := buildNaraInfo(context.Background(), people, naraOwn, naraGroup, "Climbing", naraMessage{Sender: naraPeer, Timestamp: 3})
	require.NoError(t, err)
	assert.Equal(t, naraPeer.String(), info.Sender)
	assert.Equal(t, naraPeer.String()+"|3", info.MessageID)
	assert.Empty(t, info.SenderName)
	assert.Empty(t, info.SenderPhone)
}

func TestBuildNaraInfoBadPortalID(t *testing.T) {
	_, err := buildNaraInfo(context.Background(), newNaraPeople(naraFixture()), naraOwn, "nonsense", "", naraMessage{Sender: naraPeer, Timestamp: 3})
	assert.Error(t, err)
}

func TestNaraPeopleLooksEachPersonUpOnce(t *testing.T) {
	fake := naraFixture()
	people := newNaraPeople(fake)
	for i := 0; i < 5; i++ {
		_, err := buildNaraInfo(context.Background(), people, naraOwn, naraGroup, "", naraMessage{Sender: naraPeer, Timestamp: uint64(i)})
		require.NoError(t, err)
	}
	assert.Equal(t, 1, fake.calls)
}

func TestSetNaraInfoNumbersPartsAndCopiesThem(t *testing.T) {
	shared := map[string]any{"com.example": true}
	image := &bridgev2.ConvertedMessagePart{ID: signalid.MakeMessagePartID(0), Type: event.EventMessage, Content: &event.MessageEventContent{MsgType: event.MsgImage}, Extra: shared}
	text := &bridgev2.ConvertedMessagePart{ID: signalid.MakeMessagePartID(1), Type: event.EventMessage, Content: &event.MessageEventContent{MsgType: event.MsgText, Body: "caption"}}
	cm := &bridgev2.ConvertedMessage{Parts: []*bridgev2.ConvertedMessagePart{image, text}}
	info := &NaraMessageInfo{V: 1, MessageID: "x|1"}

	setNaraInfo(cm, info)

	for i, part := range cm.Parts {
		block := part.Extra[NaraContentKey].(*NaraMessageInfo)
		assert.Equal(t, i, block.Part)
		assert.Equal(t, 2, block.Parts)
		assert.Equal(t, "x|1", block.MessageID)
	}
	// The original parts and their maps are untouched.
	assert.NotContains(t, shared, NaraContentKey)
	assert.Nil(t, text.Extra)
	assert.Equal(t, true, cm.Parts[0].Extra["com.example"])

	// Part 0 is in the JSON too, not left out as empty.
	raw, err := json.Marshal(cm.Parts[0].Extra[NaraContentKey])
	require.NoError(t, err)
	assert.Contains(t, string(raw), `"part":0`)
	assert.Contains(t, string(raw), `"from_me":false`)
	assert.NotContains(t, string(raw), "peer_phone")
}

func TestSetNaraInfoNil(t *testing.T) {
	setNaraInfo(nil, &NaraMessageInfo{})
	cm := &bridgev2.ConvertedMessage{Parts: []*bridgev2.ConvertedMessagePart{{}}}
	setNaraInfo(cm, nil)
	assert.Nil(t, cm.Parts[0].Extra)
}

func TestSetNaraEditInfoNumbersTheEditedPart(t *testing.T) {
	existing := []*database.Message{{ID: "x|1", PartID: ""}, {ID: "x|1", PartID: "1"}}
	edit := &bridgev2.ConvertedEdit{ModifiedParts: []*bridgev2.ConvertedEditPart{{Part: &database.Message{ID: "x|2", PartID: "1"}}}}

	setNaraEditInfo(edit, &NaraMessageInfo{V: 1, MessageID: "x|2", TargetID: "x|1"}, existing)

	block := edit.ModifiedParts[0].TopLevelExtra[NaraContentKey].(*NaraMessageInfo)
	assert.Equal(t, 1, block.Part)
	assert.Equal(t, 2, block.Parts)
	assert.Equal(t, "x|1", block.TargetID)
	assert.Nil(t, edit.ModifiedParts[0].Extra)
}

func TestNaraPartIndex(t *testing.T) {
	assert.Equal(t, 0, naraPartIndex(""))
	assert.Equal(t, 3, naraPartIndex(signalid.MakeMessagePartID(3)))
	assert.Equal(t, 0, naraPartIndex("editstub"))
}
