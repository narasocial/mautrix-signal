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
	"cmp"
	"context"
	"maps"
	"strconv"
	"strings"

	"github.com/google/uuid"
	"github.com/rs/zerolog"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"

	"go.mau.fi/mautrix-signal/pkg/libsignalgo"
	"go.mau.fi/mautrix-signal/pkg/signalid"
	"go.mau.fi/mautrix-signal/pkg/signalmeow/store"
	"go.mau.fi/mautrix-signal/pkg/signalmeow/types"
)

// NaraContentKey is the content key of the block every message and edit event carries for Nara
// (https://nara.contact), whose appservice reads the bridge's events off the homeserver.
// It says whose Signal an event came through, which chat it's in, who sent it, the receiver's own
// name and number for them, and Signal's message ID, none of which a Matrix event otherwise carries
// (ghosts are shared by every login on the bridge). Reactions and redactions have none: Nara finds
// them by the event they relate to.
const NaraContentKey = "social.nara"

// NaraMessageInfo is the social.nara block, version 1. Phone numbers are E.164 with a leading +.
type NaraMessageInfo struct {
	V int `json:"v"`
	// The login the event came through: the human's own ACI, as in the room's m.bridge state.
	Receiver string `json:"receiver"`
	// The portal ID, as in the room's m.bridge state: the other person's ACI (or PNI:<uuid>) for a
	// DM, the receiver's own ACI for Note to Self, the 44-character group ID for a group.
	Chat     string `json:"chat"`
	ChatName string `json:"chat_name,omitempty"` // a group's title
	Direct   bool   `json:"direct"`
	// In a DM, the other person: their number, the receiver's own name for them (the nickname they
	// gave them in Signal, else their address book name) and the name on their Signal profile.
	PeerPhone       string `json:"peer_phone,omitempty"`
	PeerName        string `json:"peer_name,omitempty"`
	PeerProfileName string `json:"peer_profile_name,omitempty"`
	// Signal's ID for this message, <sender ACI>|<sent timestamp in ms>, and in an edit, the ID of the
	// message it changes. An edit's own ID is its own timestamp.
	MessageID string `json:"message_id"`
	TargetID  string `json:"target_id,omitempty"`
	// Which of the message's events this is (0-based), and how many it has: a Signal message is one
	// event per attachment or contact, then one for its text.
	Part  int `json:"part"`
	Parts int `json:"parts"`
	// The sender's ACI, number, the receiver's name for them and their profile name.
	Sender            string `json:"sender"`
	SenderPhone       string `json:"sender_phone,omitempty"`
	SenderName        string `json:"sender_name,omitempty"`
	SenderProfileName string `json:"sender_profile_name,omitempty"`
	FromMe            bool   `json:"from_me"`
	// True for a message backfilled from the history transfer, false for one that arrived live.
	History bool `json:"history"`
}

// naraMessage is what a NaraMessageInfo is built from.
type naraMessage struct {
	Sender      uuid.UUID
	Timestamp   uint64
	TargetStamp uint64 // in an edit, the sent timestamp of the message it changes
	History     bool
}

// naraRecipients is the part of the login's signalmeow store the block reads. Every login has its
// own recipients, so the names and numbers are the receiver's own.
type naraRecipients interface {
	LoadAndUpdateRecipient(ctx context.Context, aci, pni uuid.UUID, updater store.RecipientUpdaterFunc) (*types.Recipient, error)
}

type naraPerson struct {
	phone       string
	name        string
	profileName string
}

// naraPeople looks people up in the receiver's store, once each.
type naraPeople struct {
	store naraRecipients
	cache map[libsignalgo.ServiceID]naraPerson
}

func newNaraPeople(st naraRecipients) *naraPeople {
	return &naraPeople{store: st, cache: make(map[libsignalgo.ServiceID]naraPerson)}
}

func (np *naraPeople) get(ctx context.Context, serviceID libsignalgo.ServiceID) naraPerson {
	if person, ok := np.cache[serviceID]; ok {
		return person
	}
	var aci, pni uuid.UUID
	switch serviceID.Type {
	case libsignalgo.ServiceIDTypeACI:
		aci = serviceID.UUID
	case libsignalgo.ServiceIDTypePNI:
		pni = serviceID.UUID
	}
	var person naraPerson
	if aci != uuid.Nil || pni != uuid.Nil {
		recipient, err := np.store.LoadAndUpdateRecipient(ctx, aci, pni, nil)
		if err != nil {
			zerolog.Ctx(ctx).Warn().Err(err).Stringer("service_id", serviceID).Msg("Failed to get recipient for the social.nara block")
		} else if recipient != nil {
			person = naraPerson{
				phone:       naraPhone(recipient.E164),
				name:        cmp.Or(recipient.Nickname, recipient.ContactName),
				profileName: recipient.Profile.Name,
			}
		}
	}
	np.cache[serviceID] = person
	return person
}

func naraPhone(e164 string) string {
	e164 = strings.TrimSpace(e164)
	if e164 == "" || strings.HasPrefix(e164, "+") {
		return e164
	}
	return "+" + e164
}

// naraInfo builds the block for a message in the portal, or returns nil if it can't.
func (s *SignalClient) naraInfo(ctx context.Context, portal *bridgev2.Portal, people *naraPeople, msg naraMessage) *NaraMessageInfo {
	if s.Client == nil || s.Client.Store == nil {
		return nil
	}
	if people == nil {
		people = newNaraPeople(s.Client.Store.RecipientStore)
	}
	info, err := buildNaraInfo(ctx, people, s.Client.Store.ACI, portal.ID, portal.Name, msg)
	if err != nil {
		zerolog.Ctx(ctx).Err(err).Msg("Failed to build the social.nara block")
		return nil
	}
	return info
}

func buildNaraInfo(ctx context.Context, people *naraPeople, ownACI uuid.UUID, portalID networkid.PortalID, portalName string, msg naraMessage) (*NaraMessageInfo, error) {
	peer, groupID, err := signalid.ParsePortalID(portalID)
	if err != nil {
		return nil, err
	}
	info := &NaraMessageInfo{
		V:         1,
		Receiver:  string(signalid.MakeUserLoginID(ownACI)),
		Chat:      string(portalID),
		Direct:    groupID == "",
		MessageID: string(signalid.MakeMessageID(msg.Sender, msg.Timestamp)),
		Sender:    msg.Sender.String(),
		FromMe:    msg.Sender == ownACI,
		History:   msg.History,
	}
	if msg.TargetStamp != 0 {
		info.TargetID = string(signalid.MakeMessageID(msg.Sender, msg.TargetStamp))
	}
	if !info.Direct {
		info.ChatName = portalName
	} else if peer.UUID != ownACI {
		// Note to Self has no peer.
		p := people.get(ctx, peer)
		info.PeerPhone, info.PeerName, info.PeerProfileName = p.phone, p.name, p.profileName
	}
	if info.FromMe {
		// The receiver's own messages: their number, no names.
		info.SenderPhone = people.get(ctx, libsignalgo.NewACIServiceID(ownACI)).phone
	} else {
		p := people.get(ctx, libsignalgo.NewACIServiceID(msg.Sender))
		info.SenderPhone, info.SenderName, info.SenderProfileName = p.phone, p.name, p.profileName
	}
	return info, nil
}

// setNaraInfo adds the block to each part of a converted message, numbering the parts.
// Parts are copied first, since some are shared.
func setNaraInfo(cm *bridgev2.ConvertedMessage, info *NaraMessageInfo) {
	if cm == nil || info == nil {
		return
	}
	for i, part := range cm.Parts {
		partInfo := *info
		partInfo.Part, partInfo.Parts = i, len(cm.Parts)
		withInfo := *part
		withInfo.Extra = maps.Clone(part.Extra)
		if withInfo.Extra == nil {
			withInfo.Extra = make(map[string]any, 1)
		}
		withInfo.Extra[NaraContentKey] = &partInfo
		cm.Parts[i] = &withInfo
	}
}

// setNaraEditInfo adds the block to the top level of each part of an edit, numbered like the
// part of the original message it changes. existing is the original message's parts.
func setNaraEditInfo(edit *bridgev2.ConvertedEdit, info *NaraMessageInfo, existing []*database.Message) {
	if edit == nil || info == nil {
		return
	}
	for _, part := range edit.ModifiedParts {
		partInfo := *info
		partInfo.Part, partInfo.Parts = naraPartIndex(part.Part.PartID), len(existing)
		if part.TopLevelExtra == nil {
			part.TopLevelExtra = make(map[string]any, 1)
		}
		part.TopLevelExtra[NaraContentKey] = &partInfo
	}
}

// naraPartIndex is the index of a message's part from its part ID: "" for the first, then its index
// (signalid.MakeMessagePartID).
func naraPartIndex(id networkid.PartID) int {
	n, err := strconv.Atoi(string(id))
	if err != nil {
		return 0
	}
	return n
}
