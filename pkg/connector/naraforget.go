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
	"fmt"
	"net/http"
	"slices"
	"time"

	"github.com/google/uuid"
	"github.com/lib/pq"
	"github.com/rs/zerolog"
	"go.mau.fi/util/dbutil"
	"go.mau.fi/util/exhttp"
	"maunium.net/go/mautrix"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/id"

	"go.mau.fi/mautrix-signal/pkg/libsignalgo"
	"go.mau.fi/mautrix-signal/pkg/signalid"
)

// Forgetting a human's chats, for Nara (https://nara.contact), as Nara's mautrix-whatsapp does. A
// logout deletes the device and everything signalmeow keeps for it (sessions, keys, recipients,
// groups, the history transfer), but leaves the bridge's ghost for everyone the human chatted with:
// their name, their number in the ghost's identifiers, their avatar. Nara's server, after logging a
// login out, finds the human's rooms on the homeserver and the ghosts in them no one else's rooms
// have, then calls POST /_matrix/provision/v3/nara/forget with both. This deletes what the bridge
// keeps of them; Nara purges the rooms and the ghosts' Matrix users.
//
// A ghost stays when the bridge still has it in use: in a room it knows of, joined or invited, as a
// live login's own ghost, or as the sender of a message or reaction in a chat that stays (deleting a
// ghost deletes those, by foreign key).
//
// The state store also keeps what it saw of every room the bot has left, which a purge's own leave
// events add to after this is called: every NaraDeadRoomsInterval, and on every call, the bridge drops
// it for each room with no portal that its bot has left.

type naraForgetRequest struct {
	Rooms  []id.RoomID `json:"rooms"`
	Ghosts []id.UserID `json:"ghosts"`
}

type naraForgetResponse struct {
	// How many of the rooms still had a portal, now deleted.
	Portals int `json:"portals"`
	// The ghosts deleted.
	Ghosts []id.UserID `json:"ghosts"`
	// The ghosts kept, still in use.
	Kept []id.UserID `json:"kept"`
}

// NaraDeadRoomsInterval is how often the bridge drops what it cached of rooms it's no longer in.
const NaraDeadRoomsInterval = 10 * time.Minute

func (s *SignalConnector) registerNaraForget() {
	prov, ok := s.Bridge.Matrix.(bridgev2.MatrixConnectorWithProvisioning)
	if !ok {
		return
	}
	go s.naraDeadRoomsLoop()
	api := prov.GetProvisioning()
	api.GetRouter().HandleFunc("POST /v3/nara/forget", func(w http.ResponseWriter, r *http.Request) {
		var req naraForgetRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			mautrix.MBadJSON.WithMessage("Invalid request body").Write(w)
			return
		}
		resp, err := s.naraForget(r.Context(), api.GetUser(r), &req)
		if err != nil {
			zerolog.Ctx(r.Context()).Err(err).Msg("Failed to forget for Nara")
			mautrix.MUnknown.WithMessage("Failed to forget").Write(w)
			return
		}
		zerolog.Ctx(r.Context()).Info().
			Int("rooms", len(req.Rooms)).
			Int("portals", resp.Portals).
			Int("ghosts", len(resp.Ghosts)).
			Int("kept", len(resp.Kept)).
			Msg("Forgot for Nara")
		exhttp.WriteJSONResponse(w, http.StatusOK, resp)
	})
}

// naraForgetIndexes let deleting a ghost find its messages and reactions (the foreign keys' cascade)
// without reading every row.
var naraForgetIndexes = []string{
	"CREATE INDEX IF NOT EXISTS nara_message_sender_idx ON message (bridge_id, sender_id)",
	"CREATE INDEX IF NOT EXISTS nara_reaction_sender_idx ON reaction (bridge_id, sender_id)",
}

func (s *SignalConnector) naraForget(ctx context.Context, user *bridgev2.User, req *naraForgetRequest) (*naraForgetResponse, error) {
	db := s.Bridge.DB.Database
	if db.Dialect != dbutil.Postgres {
		return nil, fmt.Errorf("forgetting needs Postgres")
	}
	bridgeID := s.Bridge.ID
	resp := &naraForgetResponse{Ghosts: []id.UserID{}, Kept: []id.UserID{}}
	for _, query := range naraForgetIndexes {
		if _, err := db.Exec(ctx, query); err != nil {
			return nil, fmt.Errorf("failed to create index: %w", err)
		}
	}

	// The rooms: any portal still there (the logout's cleanup runs in the background), unless it's a
	// live login's, and what the state store cached about them.
	for _, roomID := range req.Rooms {
		portal, err := s.Bridge.GetPortalByMXID(ctx, roomID)
		if err != nil {
			return nil, fmt.Errorf("failed to get portal: %w", err)
		}
		if portal == nil {
			continue
		}
		if portal.Receiver == "" || s.Bridge.GetCachedUserLoginByID(portal.Receiver) != nil {
			continue
		}
		if err = portal.Delete(ctx); err != nil {
			return nil, fmt.Errorf("failed to delete portal: %w", err)
		}
		resp.Portals++
	}
	rooms := make([]string, len(req.Rooms))
	for i, roomID := range req.Rooms {
		rooms[i] = roomID.String()
	}
	if _, err := db.Exec(ctx, "DELETE FROM mx_user_profile WHERE room_id = ANY($1)", pq.Array(rooms)); err != nil {
		return nil, fmt.Errorf("failed to delete cached members: %w", err)
	}
	if _, err := db.Exec(ctx, "DELETE FROM mx_room_state WHERE room_id = ANY($1)", pq.Array(rooms)); err != nil {
		return nil, fmt.Errorf("failed to delete cached room state: %w", err)
	}
	if user != nil && user.ManagementRoom != "" && slices.Contains(req.Rooms, user.ManagementRoom) && len(user.GetUserLoginIDs()) == 0 {
		user.ManagementRoom = ""
		if err := user.Save(ctx); err != nil {
			return nil, fmt.Errorf("failed to clear management room: %w", err)
		}
	}

	// The ghosts: an ACI's, or a PNI's (pni_<uuid>) for someone whose ACI the bridge didn't know.
	candidates := make(map[networkid.UserID]id.UserID, len(req.Ghosts))
	for _, mxid := range req.Ghosts {
		if ghostID, ok := s.Bridge.Matrix.ParseGhostMXID(mxid); ok {
			candidates[ghostID] = mxid
		}
	}
	inUse := make(map[networkid.UserID]bool)
	for _, login := range s.Bridge.GetAllCachedUserLogins() {
		// A login's ID is its account's ACI, as is its own ghost's.
		inUse[networkid.UserID(login.ID)] = true
		if client, ok := login.Client.(*SignalClient); ok && client.Client != nil && client.Client.Store.PNI != uuid.Nil {
			inUse[signalid.MakeUserIDFromServiceID(libsignalgo.NewPNIServiceID(client.Client.Store.PNI))] = true
		}
	}
	ghostIDs := make([]string, 0, len(candidates))
	ghostMXIDs := make([]string, 0, len(candidates))
	for ghostID, mxid := range candidates {
		ghostIDs = append(ghostIDs, string(ghostID))
		ghostMXIDs = append(ghostMXIDs, mxid.String())
	}
	for _, query := range []string{
		"SELECT DISTINCT sender_id FROM message WHERE bridge_id = $1 AND sender_id = ANY($2)",
		"SELECT DISTINCT sender_id FROM reaction WHERE bridge_id = $1 AND sender_id = ANY($2)",
	} {
		if err := naraCollect(ctx, db, inUse, func(v string) networkid.UserID { return networkid.UserID(v) }, query, bridgeID, pq.Array(ghostIDs)); err != nil {
			return nil, err
		}
	}
	err := naraCollect(ctx, db, inUse, func(v string) networkid.UserID {
		ghostID, _ := s.Bridge.Matrix.ParseGhostMXID(id.UserID(v))
		return ghostID
	}, "SELECT DISTINCT user_id FROM mx_user_profile WHERE user_id = ANY($1) AND membership IN ('join', 'invite')", pq.Array(ghostMXIDs))
	if err != nil {
		return nil, err
	}

	var forgottenACIs []string
	var forgottenMXIDs []string
	for ghostID, mxid := range candidates {
		if inUse[ghostID] {
			resp.Kept = append(resp.Kept, mxid)
			continue
		}
		if err = s.Bridge.DeleteGhost(ctx, ghostID); err != nil {
			return nil, fmt.Errorf("failed to delete ghost: %w", err)
		}
		resp.Ghosts = append(resp.Ghosts, mxid)
		forgottenMXIDs = append(forgottenMXIDs, mxid.String())
		if aci, err := signalid.ParseUserID(ghostID); err == nil {
			forgottenACIs = append(forgottenACIs, aci.String())
		}
	}
	if _, err = db.Exec(ctx, "DELETE FROM mx_registrations WHERE user_id = ANY($1)", pq.Array(forgottenMXIDs)); err != nil {
		return nil, fmt.Errorf("failed to delete registrations: %w", err)
	}
	if _, err = db.Exec(ctx, "DELETE FROM mx_user_profile WHERE user_id = ANY($1)", pq.Array(forgottenMXIDs)); err != nil {
		return nil, fmt.Errorf("failed to delete cached profiles: %w", err)
	}
	// Whether someone left Signal is kept for the whole bridge, not per device. A device still logged
	// in that has them as a recipient asks Signal again the next time it needs to know.
	if _, err = db.Exec(ctx, "DELETE FROM signalmeow_unregistered_users WHERE aci_uuid = ANY($1::uuid[])", pq.Array(forgottenACIs)); err != nil {
		return nil, fmt.Errorf("failed to delete unregistered users: %w", err)
	}
	if err = s.naraDropDeadRooms(ctx); err != nil {
		return nil, err
	}
	return resp, nil
}

func (s *SignalConnector) naraDeadRoomsLoop() {
	ctx := s.Bridge.BackgroundCtx
	log := s.Bridge.Log.With().Str("component", "nara_dead_rooms").Logger()
	ticker := time.NewTicker(NaraDeadRoomsInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := s.naraDropDeadRooms(ctx); err != nil {
				log.Err(err).Msg("Failed to drop dead rooms' cached state")
			}
		}
	}
}

// naraDropDeadRooms deletes the state store's rows for every room the bot has left that has no
// portal and isn't anyone's management room: who was in it and its power levels, which name people.
func (s *SignalConnector) naraDropDeadRooms(ctx context.Context) error {
	db := s.Bridge.DB.Database
	if db.Dialect != dbutil.Postgres {
		return nil
	}
	_, err := db.Exec(ctx, `
		WITH dead AS (
			SELECT p.room_id FROM mx_user_profile p
			WHERE p.user_id = $1 AND p.membership IN ('leave', 'ban')
			  AND NOT EXISTS (SELECT 1 FROM portal WHERE bridge_id = $2 AND mxid = p.room_id)
			  AND NOT EXISTS (SELECT 1 FROM "user" WHERE bridge_id = $2 AND management_room = p.room_id)
		), state AS (
			DELETE FROM mx_room_state WHERE room_id IN (SELECT room_id FROM dead)
		)
		DELETE FROM mx_user_profile WHERE room_id IN (SELECT room_id FROM dead)
	`, s.Bridge.Bot.GetMXID(), s.Bridge.ID)
	if err != nil {
		return fmt.Errorf("failed to drop dead rooms' cached state: %w", err)
	}
	return nil
}

// naraCollect sets set[parse(value)] for every value the query's first column returns.
func naraCollect[K comparable](ctx context.Context, db *dbutil.Database, set map[K]bool, parse func(string) K, query string, args ...any) error {
	rows, err := db.Query(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("failed to query: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var value string
		if err = rows.Scan(&value); err != nil {
			return err
		}
		set[parse(value)] = true
	}
	return rows.Err()
}
