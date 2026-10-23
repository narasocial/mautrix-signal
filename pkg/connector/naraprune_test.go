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
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mau.fi/util/dbutil"
	"go.mau.fi/util/jsontime"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"

	"go.mau.fi/mautrix-signal/pkg/libsignalgo"
	"go.mau.fi/mautrix-signal/pkg/signalid"
	"go.mau.fi/mautrix-signal/pkg/signalmeow/protobuf/backuppb"
	"go.mau.fi/mautrix-signal/pkg/signalmeow/store"
)

// NARA_TEST_POSTGRES is a Postgres URI the test may make a schema in (nara_prune_test), e.g.
// postgres://user:password@localhost/db?sslmode=disable. Without it, the test is skipped.
func naraPruneTestDB(t *testing.T) *dbutil.Database {
	t.Helper()
	uri := os.Getenv("NARA_TEST_POSTGRES")
	if uri == "" {
		t.Skip("NARA_TEST_POSTGRES not set")
	}
	ctx := context.Background()
	admin, err := dbutil.NewWithDialect(uri, "postgres")
	require.NoError(t, err)
	_, err = admin.Exec(ctx, "DROP SCHEMA IF EXISTS nara_prune_test CASCADE; CREATE SCHEMA nara_prune_test")
	require.NoError(t, err)
	_ = admin.Close()
	sep := "?"
	if strings.Contains(uri, "?") {
		sep = "&"
	}
	db, err := dbutil.NewWithDialect(uri+sep+"search_path=nara_prune_test", "postgres")
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func TestNaraPruneArchives(t *testing.T) {
	ctx := context.Background()
	db := naraPruneTestDB(t)
	const bridgeID = networkid.BridgeID("signal")
	require.NoError(t, database.New(bridgeID, database.MetaTypes{
		UserLogin: func() any { return &signalid.UserLoginMetadata{} },
	}, db).Upgrade(ctx))
	container := store.NewStore(db, dbutil.NoopLogger)
	require.NoError(t, container.Upgrade(ctx))
	_, err := db.Exec(ctx, `INSERT INTO "user" (bridge_id, mxid) VALUES ($1, '@h:nara.localhost')`, bridgeID)
	require.NoError(t, err)

	// A login with its transfer archive: one person, one chat, one message.
	login := func(meta *signalid.UserLoginMetadata) *store.Device {
		t.Helper()
		aci, pni := uuid.New(), uuid.New()
		aciKey, err := libsignalgo.GenerateIdentityKeyPair()
		require.NoError(t, err)
		pniKey, err := libsignalgo.GenerateIdentityKeyPair()
		require.NoError(t, err)
		require.NoError(t, container.PutDevice(ctx, &store.DeviceData{
			ACIIdentityKeyPair: aciKey, PNIIdentityKeyPair: pniKey, ACI: aci, PNI: pni, DeviceID: 2,
		}))
		device, err := container.DeviceByACI(ctx, aci)
		require.NoError(t, err)
		metaJSON, err := json.Marshal(meta)
		require.NoError(t, err)
		_, err = db.Exec(ctx, `
			INSERT INTO user_login (bridge_id, user_mxid, id, remote_name, metadata) VALUES ($1, '@h:nara.localhost', $2, '', $3)
		`, bridgeID, signalid.MakeUserLoginID(aci), metaJSON)
		require.NoError(t, err)
		friend := uuid.New()
		require.NoError(t, device.BackupStore.AddBackupRecipient(ctx, &backuppb.Recipient{
			Id: 1, Destination: &backuppb.Recipient_Contact{Contact: &backuppb.Contact{Aci: friend[:]}},
		}))
		require.NoError(t, device.BackupStore.AddBackupChat(ctx, &backuppb.Chat{Id: 1, RecipientId: 1}))
		require.NoError(t, device.BackupStore.AddBackupChatItem(ctx, &backuppb.ChatItem{
			ChatId: 1, AuthorId: 1, DateSent: 1760000000000,
			Item: &backuppb.ChatItem_StandardMessage{StandardMessage: &backuppb.StandardMessage{
				Text: &backuppb.Text{Body: "words"},
			}},
		}))
		require.NoError(t, device.BackupStore.RecalculateChatCounts(ctx))
		return device
	}
	synced := login(&signalid.UserLoginMetadata{ChatsSynced: true, NaraHistory: NaraHistoryDone})
	// Synced today: a chat whose room is still to come reads the archive, so it waits out the day.
	today := naraLoginMetadata(true)
	markNaraTransferReceived(today)
	today.ChatsSynced = true
	syncedToday := login(today)
	received := naraLoginMetadata(true)
	markNaraTransferReceived(received)
	syncing := login(received)
	stale := naraLoginMetadata(true)
	markNaraTransferReceived(stale)
	stale.NaraTransferReceivedAt = jsontime.U(time.Now().Add(-NaraArchiveMaxAge - time.Minute))
	neverSynced := login(stale)

	messages, chats, err := naraPruneArchives(ctx, db, bridgeID, time.Now())
	require.NoError(t, err)
	assert.Equal(t, int64(2), messages)
	assert.Equal(t, int64(2), chats)

	archive := func(device *store.Device) (rows int) {
		t.Helper()
		require.NoError(t, db.QueryRow(ctx, `
			SELECT (SELECT COUNT(*) FROM signalmeow_backup_message WHERE account_id = $1)
			     + (SELECT COUNT(*) FROM signalmeow_backup_chat WHERE account_id = $1)
			     + (SELECT COUNT(*) FROM signalmeow_backup_recipient WHERE account_id = $1)
		`, device.ACI.String()).Scan(&rows))
		return
	}
	assert.Equal(t, 0, archive(synced), "the archive of a login synced before the bridge recorded its transfer")
	assert.Equal(t, 3, archive(syncedToday), "the archive of a login synced today")
	assert.Equal(t, 0, archive(neverSynced), "the archive of a login whose transfer came a day ago")
	assert.Equal(t, 3, archive(syncing), "the archive of a login still syncing its chats")
	chatsLeft, err := syncing.BackupStore.GetBackupChats(ctx)
	require.NoError(t, err)
	assert.Len(t, chatsLeft, 1)
}
