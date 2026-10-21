# Nara's mautrix-signal

This is the bridge [Nara](https://nara.contact) runs for testers who connect Signal: [mautrix-signal](https://github.com/mautrix/signal) with a few patches, on the `nara` branch. It's published here under the AGPL-3.0, like the original.

The branch starts from upstream's v0.2610.0 tag (v26.10) and adds:

- **The `social.nara` block** on every message part and edit event: whose Signal it came through, the chat, Signal's message ID (`<sender ACI>|<timestamp>`) and which of the message's parts the event is, the sender's ACI, and the receiver's own number and names for the sender and the DM's other person, from that login's recipient store. Ghosts are shared by every login on the bridge, so none of that can go in their profiles. See `pkg/connector/nara.go`.
- **History in bridge states.** Each login's bridge state info has `history`: `"started"` from the link until every chat in the phone's history transfer has its portal and backfill, then `"done"`, and `history_transfer`: `"received"`, `"none"` (the phone sent none), the phone's reason for refusing it, or `"failed"` (history then `"done"` too) when it never came within an hour or couldn't be read. The bridge tries again on its next start; if the transfer comes then, history is `"started"` again until its chats are synced. See `pkg/connector/nara_history.go`.
- **A `redacted-stdout` log writer** (`cmd/mautrix-signal/redactlog.go`, the same as Nara's mautrix-whatsapp's): in each JSON log line, a run of 7 or more digits (a phone number) becomes a keyed hash that's the same for one run of the bridge, and any name or message text is dropped. Upstream logs a whole contact, name and number, whenever its contact sync skips one, so a hosting provider's logs would hold testers' contacts.
- **[narasocial/mautrix-go](https://github.com/narasocial/mautrix-go)'s `nara` branch** in place of mautrix-go (`replace` in `go.mod`): a backfill skips a message the homeserver refuses (like one over the 64 KiB event limit) instead of ending the chat's backfill.

Nara builds its image from a pinned commit of this branch (its `bridges/signal/Dockerfile`), the way upstream's Dockerfile does.
