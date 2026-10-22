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
	"fmt"
	"time"

	"go.mau.fi/util/dbutil"
	"maunium.net/go/mautrix/bridgev2/networkid"
)

// Keeping none of the phone's history transfer once its chats are synced, for Nara
// (https://nara.contact).
//
// The transfer archive (signalmeow_backup_message, _chat and _recipient: every chat's messages with
// their text, and the people and groups they're from) is read when the login's chats are synced:
// syncChats makes each chat's portal, whose backfill reads that chat's messages and deletes them once
// it's sent. Some chats never get that far, and their messages stayed until the login logged out:
// one whose recipient or group couldn't be read (like a group the human has left), one whose portal
// was already there (the resync backfills nothing older than what it has) or couldn't be made, one
// whose backfill failed. And the recipients always stayed.
//
// syncChats marks the login's chats synced once every portal it queued has made its room and sent its
// backfill (or an hour has passed), and never reads the archive again then. So while the backfill
// queue is off, at start and every NaraPruneInterval, the bridge deletes the archive of every login
// whose chats are synced.

// NaraPruneInterval is how often the bridge deletes history transfer archives nothing will read.
const NaraPruneInterval = 10 * time.Minute

func (s *SignalConnector) naraPruneLoop() {
	ctx := s.Bridge.BackgroundCtx
	log := s.Bridge.Log.With().Str("component", "nara_prune").Logger()
	if s.Bridge.Config.Backfill.Queue.AnyEnabled() {
		log.Debug().Msg("Not pruning history transfer archives, as the backfill queue reads them")
		return
	} else if s.Bridge.DB.Dialect != dbutil.Postgres {
		log.Warn().Msg("Not pruning history transfer archives, which needs Postgres")
		return
	}
	ticker := time.NewTicker(NaraPruneInterval)
	defer ticker.Stop()
	for {
		messages, chats, err := naraPruneArchives(ctx, s.Bridge.DB.Database, s.Bridge.ID)
		if err != nil {
			log.Err(err).Msg("Failed to prune history transfer archives")
		} else if messages > 0 || chats > 0 {
			log.Info().
				Int64("messages", messages).
				Int64("chats", chats).
				Msg("Pruned history transfer archives of synced logins")
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// naraPruneArchives deletes the history transfer archive of every login of the bridge whose chats are
// synced, and says how many messages and chats it deleted.
func naraPruneArchives(ctx context.Context, db *dbutil.Database, bridgeID networkid.BridgeID) (messages, chats int64, err error) {
	const synced = `
		SELECT id FROM user_login WHERE bridge_id = $1 AND metadata @> '{"chats_synced": true}'
	`
	err = db.DoTxn(ctx, nil, func(ctx context.Context) error {
		res, err := db.Exec(ctx, `DELETE FROM signalmeow_backup_message WHERE account_id IN (`+synced+`)`, bridgeID)
		if err != nil {
			return fmt.Errorf("failed to delete messages: %w", err)
		}
		messages, _ = res.RowsAffected()
		res, err = db.Exec(ctx, `DELETE FROM signalmeow_backup_chat WHERE account_id IN (`+synced+`)`, bridgeID)
		if err != nil {
			return fmt.Errorf("failed to delete chats: %w", err)
		}
		chats, _ = res.RowsAffected()
		_, err = db.Exec(ctx, `DELETE FROM signalmeow_backup_recipient WHERE account_id IN (`+synced+`)`, bridgeID)
		if err != nil {
			return fmt.Errorf("failed to delete recipients: %w", err)
		}
		return nil
	})
	return
}
