package matrix

import (
	"context"
	"fmt"
	"html"
	"math"
	"net/url"
	"path"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/yunhu/yhmatrix/internal/model"
	"github.com/yunhu/yhmatrix/internal/yunhu"
	"github.com/yunhu/yhmatrix/internal/yunhu/pb"
)

// This file is the translation layer between Yunhu messages and Matrix events.

// ---------------------------------------------------------------------------
// Yunhu -> Matrix
// ---------------------------------------------------------------------------

// conversationOf works out which conversation a pushed message belongs to.
//
// The push carries chat_id, chat_type and recv_id, and their meaning depends on
// the conversation kind - Yunhu's own client decides it with
//
//	isPrivateChat = message.chatId == message.recvId
//	targetChatId  = if (isPrivateChat) message.sender.chatId else message.chatId
//	targetChatType= if (isPrivateChat) message.sender.chatType else message.chatType
//
// Taking chat_id at face value is what made a private message from a bot open a
// *second*, empty room: for a 1:1 push chat_id and recv_id are both the account's
// own id, and the conversation is the peer (the bot). The message then landed in
// !yh-user-<own id> instead of !yh-bot-<bot id>, so the client showed a brand new
// empty chat carrying the bot's text.
//
// Returns the conversation id and type. ok is false when the push does not say
// enough to tell (no chat_id and no usable sender).
func conversationOf(m *pb.PushMessage, acc *model.Account) (chatID string, chatType int32, ok bool) {
	chatID = m.GetChatId()
	chatType = int32(m.GetChatType())
	recvID := m.GetRecvId()
	senderID := m.GetSender().GetChatId()
	senderType := int32(m.GetSender().GetChatType())

	// A missing chat_type is common on bot pushes and on edits. Resolve it from
	// what the account already knows *before* deciding what kind of push this is,
	// otherwise a group message with no type was mistaken for a 1:1 one.
	if chatType == 0 && chatID != "" {
		chatType = inferTypeFromID(acc, chatID)
	}
	if chatType == 0 && senderID != "" {
		chatType = inferTypeFromID(acc, senderID)
	}

	// A 1:1 conversation is the one where the recipient is us. Yunhu sets both
	// chat_id and recv_id to the account's id in that case.
	//
	// chat_id == recv_id alone is NOT enough: a group message also has
	// recv_id == chat_id (the group is its own recipient). The discriminator is
	// chat_type - a group push says group, a 1:1 push says user (or bot).
	// Without this check every group message was re-routed to the sender's 1:1
	// conversation.
	isPrivate := recvID != "" && chatID == recvID && chatType != model.ChatTypeGroup

	if isPrivate && senderID != "" {
		// The peer owns the conversation, not us.
		if acc == nil || senderID != acc.YunhuID {
			t := senderType
			if t == model.ChatTypeGroup || t == 0 {
				// A 1:1 partner is a user or a bot, never a group. Yunhu omits
				// the type on many bot pushes, and the bot's own id names it.
				t = model.ChatTypeBot
				if acc != nil {
					if room := acc.RoomByChatID(senderID); room != nil {
						t = room.ChatType
					}
				}
			}
			return senderID, t, true
		}
	}

	if chatID == "" {
		return "", 0, false
	}
	// A group (or an outgoing 1:1 whose peer we cannot name): chat_id is the
	// conversation.
	return chatID, chatType, true
}

// inferTypeFromID recovers a conversation's kind from what the account already
// knows about it, defaulting to "user" for an unknown id.
func inferTypeFromID(acc *model.Account, chatID string) int32 {
	if acc != nil {
		if room := acc.RoomByChatID(chatID); room != nil {
			return room.ChatType
		}
	}
	return model.ChatTypeUser
}

// senderUserID maps the sender of a Yunhu message onto a Matrix user ID.
//
// The sender type is missing from some pushes (bots and a few server-side
// messages come through with chat_type unset). Falling back to the *room* type
// there produced @yhgrp_<botid> senders in groups — a "group" that nobody can
// render, with no matching member event. Groups never send messages, so the
// fallback is the room type only for DMs and a plain user otherwise.
func (s *Server) senderUserID(room *model.Room, m *pb.PushMessage) string {
	senderID := m.GetSender().GetChatId()
	if senderID == "" {
		senderID = room.ChatID
	}
	t := int32(m.GetSender().GetChatType())
	switch t {
	case model.ChatTypeUser, model.ChatTypeBot:
		// Explicit: keep it.
	case model.ChatTypeGroup:
		// Only the room itself may legitimately send as a group.
		if senderID != room.ChatID {
			t = model.ChatTypeUser
		}
	default:
		// Missing type. A DM partner keeps the room's type (so a bot DM still
		// renders as a bot); a group member is a user, never the group.
		if room.ChatType == model.ChatTypeBot {
			t = model.ChatTypeBot
		} else {
			t = model.ChatTypeUser
		}
	}
	return s.mxUserID(senderID, t)
}

// yunhuToEvent projects an upstream message onto a Matrix event.
//
// For an edit the returned event is the m.replace *relation* event: it carries
// the new content under m.new_content and points at the original through
// m.relates_to. The caller (ingest / appendHistoryLocked) is responsible for
// keeping the original event in place and stamping its EditedTS - that split is
// what lets a client show one bubble with an "edited" marker instead of two
// messages, one of them prefixed with "* ".
func (s *Server) yunhuToEvent(room *model.Room, cmd string, m *pb.PushMessage, pos int64) *model.Event {
	sender := s.senderUserID(room, m)

	// The room is already in hand, so the bot resolver is built from it instead
	// of querying the store: this runs under the store lock in ingest().
	content := s.buildContent(m.GetContentType(), m.GetContent(), botNameResolver(room))
	if m.GetQuoteMsgId() != "" {
		content["m.relates_to"] = map[string]any{
			"m.in_reply_to": map[string]any{"event_id": eventIDFor(m.GetQuoteMsgId())},
		}
	}

	isEdit := cmd == "edit_message"
	if isEdit {
		// Matrix spells an edit as a new event whose m.new_content holds the
		// replacement and whose body is the fallback "* new text" a client shows
		// only when it cannot apply the relation.
		relates := map[string]any{
			"rel_type": "m.replace",
			"event_id": eventIDFor(m.GetMsgId()),
		}
		content = map[string]any{
			"msgtype":       orDefault(asString(content["msgtype"]), "m.text"),
			"body":          editFallbackBody(content),
			"m.new_content": content,
			"m.relates_to":  relates,
		}
	}

	// Edits and recalls get their own event so the original stays intact and
	// clients can apply the m.replace relation.
	dedupeKey := m.GetMsgId()
	switch cmd {
	case "edit_message":
		// One editable event per message: repeated edits refresh it in place.
		dedupeKey = m.GetMsgId() + "_edit"
	case "blocked_message":
		dedupeKey = fmt.Sprintf("%s_recall%d", m.GetMsgId(), m.GetDeleteTimestamp())
	}

	ts := int64(m.GetTimestamp())
	if ts == 0 {
		ts = time.Now().UnixMilli()
	}
	if ts < 1e12 { // seconds instead of milliseconds
		ts *= 1000
	}

	ev := &model.Event{
		EventID:    eventIDFor(dedupeKey),
		Type:       "m.room.message",
		Sender:     sender,
		Content:    content,
		OriginTS:   ts,
		Pos:        pos,
		YunhuMsgID: dedupeKey,
	}
	if cmd == "blocked_message" || m.GetDeleteTimestamp() > 0 {
		ev.Unsigned = map[string]any{"yunhu.recalled": true}
	}
	return ev
}

// editFallbackBody renders "* <text>" for an edit's fallback body, which is what
// Matrix clients that cannot resolve the relation display.
func editFallbackBody(newContent map[string]any) string {
	body := asString(newContent["body"])
	if body == "" {
		// Media edits have no body worth prefixing; the fallback just says what
		// the new message is.
		body = orDefault(asString(newContent["msgtype"]), "message")
	}
	return "* " + body
}

// addMediaInfo fills in the fields clients need but Yunhu never sends: the MIME
// type and the byte size. A zero size is deliberately never emitted — Element
// renders `"size": 0` as "0 B", which is what made images and voice notes look
// like empty files.
func (s *Server) addMediaInfo(info map[string]any, ref string) {
	if _, set := info["size"]; !set {
		if size := s.Media.CachedSize(ref); size > 0 {
			info["size"] = size
		}
	}
	if _, set := info["mimetype"]; !set {
		if mime := mediaMime(ref); mime != "" {
			info["mimetype"] = mime
		}
	}
}

// setDimensions records the image size only when Yunhu actually reported one.
// A zero width/height makes clients compute a 0/0 aspect ratio and collapse the
// image to nothing, so saying nothing is better: the client then measures the
// file it downloaded.
func setDimensions(info map[string]any, w, h int32) {
	if w > 0 {
		info["w"] = w
	}
	if h > 0 {
		info["h"] = h
	}
}

// mediaMime derives a MIME type from a Yunhu media reference. The payload has no
// MIME field for images, voice notes or videos, and clients use it to decide how
// to render (and whether they can play) the media.
func mediaMime(ref string) string {
	if i := strings.IndexAny(ref, "?#"); i >= 0 {
		ref = ref[:i]
	}
	switch strings.ToLower(path.Ext(ref)) {
	case ".jpg", ".jpeg":
		return "image/jpeg"
	case ".png":
		return "image/png"
	case ".gif":
		return "image/gif"
	case ".webp":
		return "image/webp"
	case ".bmp":
		return "image/bmp"
	case ".svg":
		return "image/svg+xml"
	case ".heic":
		return "image/heic"
	case ".mp4":
		return "video/mp4"
	case ".mov":
		return "video/quicktime"
	case ".webm":
		return "video/webm"
	case ".m4a":
		return "audio/mp4"
	case ".mp3":
		return "audio/mpeg"
	case ".ogg", ".opus":
		return "audio/ogg"
	case ".wav":
		return "audio/wav"
	case ".amr":
		return "audio/amr"
	case ".pdf":
		return "application/pdf"
	case ".zip":
		return "application/zip"
	case ".txt":
		return "text/plain"
	}
	return ""
}

// mediaFileName picks a display name from the reference when Yunhu sends none,
// so a .gif is not labelled "image.jpg".
func mediaFileName(ref, fallback string) string {
	if i := strings.IndexAny(ref, "?#"); i >= 0 {
		ref = ref[:i]
	}
	base := path.Base(ref)
	if base == "" || base == "." || base == "/" || !strings.Contains(base, ".") {
		return fallback
	}
	if unescaped, err := url.PathUnescape(base); err == nil {
		base = unescaped
	}
	return base
}

// videoBody picks the descriptive text of a video message.
//
// Matrix shows `body` as the caption, so it should be what the sender wrote. Yunhu
// puts a caption in text alongside video_url, but when there is none the file
// name is the only thing left - and a client renders an empty bubble without it,
// so fall back to the derived name rather than sending "".
func videoBody(c *pb.MsgContent, ref string) string {
	if text := strings.TrimSpace(c.GetText()); text != "" {
		return text
	}
	if name := strings.TrimSpace(c.GetFileName()); name != "" {
		return name
	}
	return mediaFileName(ref, "video.mp4")
}

// buildContent maps a Yunhu content type onto Matrix message content.
// buildContent renders a Yunhu message payload as Matrix message content.
//
// resolveBot, when given, maps a mentioned display name to a known bot id. It is
// a parameter rather than a store lookup on purpose: this runs inside ingest()
// while the store lock is held, and sync.RWMutex is not reentrant.
func (s *Server) buildContent(ct pb.ContentType, c *pb.MsgContent, resolveBot ...func(string) string) map[string]any {
	var botOf func(string) string
	if len(resolveBot) > 0 {
		botOf = resolveBot[0]
	}
	if c == nil {
		return map[string]any{"msgtype": "m.text", "body": ""}
	}

	// Yunhu's own client marks a video with content_type = 10, but older payloads
	// and forwarded messages carry video_url on a text message instead. Both
	// shapes have to become m.video, so the check keys on video_url being present
	// (or on the dedicated type) rather than on the type alone.
	//
	// A type-10 message with no url is not a video we can render: fall through so
	// it becomes whatever the content actually holds instead of an m.video with
	// an empty url, which no client can play.
	if c.GetVideoUrl() != "" {
		ref := c.GetVideoUrl()
		info := map[string]any{}
		if t := c.GetVideoTime(); t > 0 {
			// Yunhu counts seconds; Matrix wants milliseconds.
			info["duration"] = t * 1000
		}
		setDimensions(info, c.GetWidth(), c.GetHeight())
		if sz := c.GetFileSize(); sz > 0 {
			info["size"] = sz
		}
		s.addMediaInfo(info, ref)
		return map[string]any{
			"msgtype": "m.video",
			"body":    videoBody(c, ref),
			"url":     s.Media.RemoteMXC(ref),
			"info":    info,
		}
	}

	switch ct {
	case pb.ContentType_text:
		return s.textContent(c, botOf)

	case pb.ContentType_markdown:
		// Markdown gets the same mention treatment: a client that receives
		// "@[名字]" in the body renders it literally, and the mention links would
		// be missing from formatted_body.
		body, formatted, mentions := s.renderMentions(c, mdToHTML, botOf)
		out := map[string]any{
			"msgtype":        "m.text",
			"body":           body,
			"format":         "org.matrix.custom.html",
			"formatted_body": formatted,
		}
		if len(mentions) > 0 {
			out["m.mentions"] = map[string]any{"user_ids": mentions}
		}
		return out

	case pb.ContentType_html:
		raw := c.GetText()
		return map[string]any{
			"msgtype":        "m.text",
			"body":           stripHTML(raw),
			"format":         "org.matrix.custom.html",
			"formatted_body": raw,
		}

	case pb.ContentType_image:
		ref := orDefault(c.GetImageUrl(), c.GetImage())
		info := map[string]any{}
		setDimensions(info, c.GetWidth(), c.GetHeight())
		if sz := c.GetFileSize(); sz > 0 {
			info["size"] = sz
		}
		s.addMediaInfo(info, ref)
		return map[string]any{
			"msgtype": "m.image",
			"body":    orDefault(c.GetFileName(), mediaFileName(ref, "image.jpg")),
			"url":     s.Media.RemoteMXC(ref),
			"info":    info,
		}

	case pb.ContentType_sticker:
		// The sticker reference is a bare Qiniu key ("sticker/<md5>.gif"),
		// resolved against chat-img.jwznb.com at download time.
		ref := orDefault(c.GetImage(), c.GetImageUrl())
		info := map[string]any{}
		setDimensions(info, c.GetWidth(), c.GetHeight())
		if sz := c.GetFileSize(); sz > 0 {
			info["size"] = sz
		}
		s.addMediaInfo(info, ref)
		return map[string]any{
			"msgtype": "m.image",
			"body":    "sticker",
			"url":     s.Media.RemoteMXC(ref),
			"info":    info,
		}

	case pb.ContentType_file:
		ref := orDefault(c.GetFileUrl(), c.GetFileName())
		name := orDefault(c.GetFileName(), "file.bin")
		info := map[string]any{}
		if sz := c.GetFileSize(); sz > 0 {
			info["size"] = sz
		}
		s.addMediaInfo(info, name)
		return map[string]any{
			"msgtype":  "m.file",
			"body":     name,
			"filename": name,
			"url":      s.Media.RemoteMXC(ref),
			"info":     info,
		}

	case pb.ContentType_audio:
		ref := c.GetAudioUrl()
		info := map[string]any{}
		if t := c.GetAudioTime(); t > 0 {
			// Yunhu counts seconds; Matrix wants milliseconds.
			info["duration"] = t * 1000
		}
		if sz := c.GetFileSize(); sz > 0 {
			info["size"] = sz
		}
		s.addMediaInfo(info, ref)
		return map[string]any{
			"msgtype": "m.audio",
			"body":    orDefault(c.GetFileName(), mediaFileName(ref, "voice.m4a")),
			"url":     s.Media.RemoteMXC(ref),
			"info":    info,
		}

	case pb.ContentType_video:
		// content_type = 10 with no video_url: nothing playable, but labelling it
		// as a text message is still better than dropping it silently.
		return map[string]any{
			"msgtype": "m.video",
			"body":    orDefault(c.GetFileName(), "video.mp4"),
		}

	case pb.ContentType_tip:
		return map[string]any{"msgtype": "m.notice", "body": orDefault(c.GetTip(), c.GetText())}

	case pb.ContentType_call:
		return map[string]any{
			"msgtype": "m.notice",
			"body":    orDefault(c.GetCallStatusText(), orDefault(c.GetRoomName(), "语音通话")),
		}

	case pb.ContentType_form:
		return map[string]any{
			"msgtype": "m.text",
			"body":    "表单消息",
			"format":  "org.matrix.custom.html",
			"formatted_body": "<pre><code class=\"language-json\">" +
				html.EscapeString(c.GetForm()) + "</code></pre>",
		}

	case pb.ContentType_a2ui:
		return map[string]any{
			"msgtype": "m.text",
			"body":    orDefault(c.GetText(), "A2UI 消息"),
		}
	}
	return map[string]any{"msgtype": "m.text", "body": c.GetText()}
}

// mentionUserIDs converts Yunhu mention targets into Matrix user IDs.
func (s *Server) mentionUserIDs(ids []string) []string {
	out := make([]string, 0, len(ids))
	for _, id := range ids {
		if id == "" {
			continue
		}
		out = append(out, s.mxUserID(id, model.ChatTypeUser))
	}
	return out
}

// yunhuMentionPattern matches Yunhu's in-text mention markup: "@[显示名]".
//
// Yunhu carries the mention twice - mentioned_id lists the targets, and the body
// spells them out as "@[name]" - but the body never says which id belongs to
// which name. The pairing has to come from the room's member list, which is why
// resolveMentions takes the room.
var yunhuMentionPattern = regexp.MustCompile(`@\[([^\[\]]*)\]`)

// textContent renders a plain text message, turning Yunhu's "@[name]" markup into
// a proper Matrix mention.
//
// A Matrix client does not understand "@[name]": it renders the brackets
// literally. The correct shape is a plain "@displayname" in the fallback body, an
// <a href="https://matrix.to/#/…"> link in formatted_body, and the user ids in
// m.mentions - which is what makes the client highlight the message and put the
// user in its mention picker.
func (s *Server) textContent(c *pb.MsgContent, resolveBot func(string) string) map[string]any {
	body, formatted, mentions := s.renderMentions(c, html.EscapeString, resolveBot)
	escaped := html.EscapeString(c.GetText())
	out := map[string]any{"msgtype": "m.text", "body": body}
	if formatted != escaped {
		// Only claim an HTML representation when the text actually contains a
		// mention: an escaped-but-identical body would make every plain message
		// carry formatted_body, which clients render differently (line breaks).
		out["format"] = "org.matrix.custom.html"
		out["formatted_body"] = formatted
	}
	if len(mentions) > 0 {
		out["m.mentions"] = map[string]any{"user_ids": mentions}
	}
	return out
}

// renderMentions rewrites "@[name]" into Matrix mention syntax.
//
// toHTML converts a run of ordinary text into its HTML form, so plain text gets
// escaped and markdown gets rendered. Mentions themselves are always rendered as
// anchors, never passed through toHTML: they carry a link, and a markdown
// renderer would either escape it or leave the brackets intact.
//
// The results are the plain-text body, the HTML body and the de-duplicated list
// of mentioned user ids.
func (s *Server) renderMentions(c *pb.MsgContent, toHTML func(string) string, resolveBot func(string) string) (string, string, []string) {
	raw := c.GetText()
	mentioned := c.GetMentionedId()

	// Build name -> user id from the explicit targets. Yunhu emits mentioned_id
	// and the "@[name]" placeholders in the same order, so when the counts line
	// up the pairs can be assigned positionally - that ordering is the only hint
	// the payload gives, since the body itself never names an id.
	byName := make(map[string]string, len(mentioned))
	if len(mentioned) > 0 {
		names := yunhuMentionPattern.FindAllStringSubmatch(raw, -1)
		if len(names) == len(mentioned) {
			for i, m := range names {
				byName[m[1]] = mentioned[i]
			}
		}
	}

	var body strings.Builder
	var formatted strings.Builder
	seen := make(map[string]struct{}, len(mentioned))
	ids := make([]string, 0, len(mentioned))

	last := 0
	emitRun := func(run string) {
		body.WriteString(run)
		formatted.WriteString(toHTML(run))
	}
	for _, loc := range yunhuMentionPattern.FindAllStringSubmatchIndex(raw, -1) {
		name := raw[loc[2]:loc[3]]
		emitRun(raw[last:loc[0]])
		last = loc[1]

		yunhuID := byName[name]
		if yunhuID == "" {
			// No id for this name: keep the text exactly as written rather than
			// dropping it, so nothing silently disappears from the message.
			emitRun(raw[loc[0]:loc[1]])
			continue
		}
		userID := s.mentionUserID(yunhuID, name, resolveBot)
		body.WriteString("@" + name)
		formatted.WriteString(`<a href="` + matrixToURL(userID) + `">` + html.EscapeString("@"+name) + `</a>`)
		if _, dup := seen[userID]; !dup {
			seen[userID] = struct{}{}
			ids = append(ids, userID)
		}
	}
	emitRun(raw[last:])

	return body.String(), formatted.String(), ids
}

// mentionUserID maps a Yunhu mention target onto the right Matrix user.
//
// mentioned_id carries no type, but a bot is not the same Matrix user as a
// person with the same numeric id: @yhbot_<id> and @yh_<id> are different
// accounts, and pointing a mention at the wrong one means the mention highlights
// nobody. The mention's own display name settles it - the name came from the
// message the other side wrote, and a bot's profile is already known from the
// messages it sent - so a name that matches a known bot makes it a bot mention.
func (s *Server) mentionUserID(yunhuID, displayName string, resolveBot func(string) string) string {
	if resolveBot != nil {
		if botID := resolveBot(displayName); botID != "" {
			return s.mxUserID(botID, model.ChatTypeBot)
		}
	}
	return s.mxUserID(yunhuID, model.ChatTypeUser)
}

// matrixToURL builds the matrix.to permalink for a user id, which is what a
// mention link in formatted_body must point at.
func matrixToURL(userID string) string {
	return "https://matrix.to/#/" + url.PathEscape(userID)
}

// ---------------------------------------------------------------------------
// Matrix mentions -> Yunhu
// ---------------------------------------------------------------------------

// mentionUserIDsFrom extracts m.mentions.user_ids from message content.
//
// Clients send the list as []any (JSON decode) or []string (a locally built
// event), so both shapes are accepted.
func mentionUserIDsFrom(content map[string]any) ([]string, bool) {
	mentions, ok := content["m.mentions"].(map[string]any)
	if !ok {
		return nil, false
	}
	switch ids := mentions["user_ids"].(type) {
	case []any:
		out := make([]string, 0, len(ids))
		for _, raw := range ids {
			if id := asString(raw); id != "" {
				out = append(out, id)
			}
		}
		return out, true
	case []string:
		return ids, true
	}
	return nil, false
}

// knownBotID is kept as the lock-taking wrapper for callers that are not
// already inside the store lock (tests and one-off lookups). Code paths that
// run under the lock must use botNameResolver instead.
func (s *Server) knownBotID(name string) string {
	s.Store.Mu.RLock()
	rooms := make([]*model.Room, 0, len(s.Store.State.Accounts))
	for _, acc := range s.Store.State.Accounts {
		for _, room := range acc.Rooms {
			rooms = append(rooms, room)
		}
	}
	s.Store.Mu.RUnlock()
	return botNameResolver(rooms...)(name)
}

// botNameResolver builds a display-name -> Yunhu bot id resolver from rooms that
// are already loaded.
//
// It deliberately does NOT take the store lock: callers like ingest() are
// already holding it, and sync.RWMutex is not reentrant, so reaching back into
// Store here would deadlock the whole homeserver on the first mention.
func botNameResolver(rooms ...*model.Room) func(string) string {
	names := map[string]string{}
	botPrefix := "@" + userPrefixBot
	// localpart strips "@" and a trailing ":server" without assuming which
	// server the id belongs to, then removes the bot prefix.
	localpart := func(uid string) string {
		s := strings.TrimPrefix(uid, "@")
		if i := strings.IndexByte(s, ':'); i > 0 {
			s = s[:i]
		}
		return strings.TrimPrefix(s, userPrefixBot)
	}
	for _, room := range rooms {
		if room == nil {
			continue
		}
		for uid, m := range room.Members {
			if !strings.HasPrefix(uid, botPrefix) || m == nil || m.DisplayName == "" {
				continue
			}
			if _, seen := names[m.DisplayName]; !seen {
				names[m.DisplayName] = localpart(uid)
			}
		}
		for key, ev := range room.State {
			if !strings.HasPrefix(key, "m.room.member\x00"+botPrefix) || ev == nil {
				continue
			}
			name := asString(ev.Content["displayname"])
			if name == "" {
				continue
			}
			if _, seen := names[name]; seen {
				continue
			}
			names[name] = localpart(strings.TrimPrefix(key, "m.room.member\x00"))
		}
	}
	return func(name string) string {
		return names[strings.TrimSpace(name)]
	}
}

// mentionNameResolver resolves a Matrix user id to the name the given room knows
// it by, for rewriting a mention into Yunhu's "@[显示名]" markup.
//
// It reads the room the message is going to, not the whole account: two groups
// can know the same person under different names, and the placeholder has to
// match what the recipients of *this* conversation see.
func mentionNameResolver(acc *model.Account, room *model.Room) func(string) string {
	names := map[string]string{}
	snapshot := func(r *model.Room) {
		if r == nil {
			return
		}
		for uid, m := range r.Members {
			if _, seen := names[uid]; seen || m == nil || m.DisplayName == "" {
				continue
			}
			names[uid] = m.DisplayName
		}
		for key, ev := range r.State {
			if !strings.HasPrefix(key, "m.room.member\x00") || ev == nil {
				continue
			}
			if _, seen := names[key]; seen {
				continue
			}
			if name := asString(ev.Content["displayname"]); name != "" {
				names[key] = name
			}
		}
	}
	snapshot(room)
	if acc != nil {
		// Fall back to the account's other rooms: a mention can name somebody who
		// has not spoken in this room yet, and their profile is still known from
		// elsewhere.
		for _, r := range acc.Rooms {
			snapshot(r)
		}
	}

	return func(userID string) string {
		if name, ok := names[userID]; ok {
			return name
		}
		// Fall back to the localpart so a mention still renders as something
		// readable instead of disappearing.
		return model.LocalpartOf(userID)
	}
}

// rewriteMentionsForYunhu turns a Matrix body into one Yunhu understands.
//
// Matrix clients put the mention in the body either as a matrix.to link, as
// "@displayname", or as a bare name, and list the truth in m.mentions. Yunhu
// wants "@[显示名]" at each spot and the ids in mentioned_id. Every listed user is
// therefore replaced by its placeholder, in the order the names appear in the
// body, so the positional pairing Yunhu uses on the way back stays correct.
//
// A name that does not occur in the body is still recorded in mentioned_id by
// the caller, but no placeholder is invented for it: adding "@[name]" text the
// user never wrote would be visible in the delivered message.
func rewriteMentionsForYunhu(body string, userIDs []string, nameOf func(string) string) string {
	out := body
	for _, uid := range userIDs {
		if uid == "" {
			continue
		}
		name := nameOf(uid)
		if name == "" {
			continue
		}
		placeholder := "@[" + name + "]"
		link := matrixToURL(uid)

		// The three shapes a client may have produced, most specific first.
		out = strings.ReplaceAll(out, link, placeholder)
		out = strings.ReplaceAll(out, "@"+name, placeholder)
		// A bare display name is only rewritten when it stands alone as a word,
		// so "Bob" inside "Bobby" is left alone.
		if !strings.Contains(out, placeholder) {
			out = replaceWholeWord(out, name, placeholder)
		}
	}
	return out
}

// replaceWholeWord replaces every standalone occurrence of word in s.
func replaceWholeWord(s, word, repl string) string {
	if word == "" {
		return s
	}
	var b strings.Builder
	for {
		i := strings.Index(s, word)
		if i < 0 {
			b.WriteString(s)
			return b.String()
		}
		before := i == 0 || isMentionBoundary(rune(s[i-1]))
		afterIdx := i + len(word)
		after := afterIdx >= len(s) || isMentionBoundary(rune(s[afterIdx]))
		if before && after {
			b.WriteString(s[:i])
			b.WriteString(repl)
		} else {
			b.WriteString(s[:afterIdx])
		}
		s = s[afterIdx:]
	}
}

// isMentionBoundary reports whether r ends a word, so a name match there is a
// whole word rather than part of a longer one.
func isMentionBoundary(r rune) bool {
	switch {
	case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9':
		return false
	case r == '_':
		return false
	}
	return true
}

// ---------------------------------------------------------------------------
// Matrix -> Yunhu
// ---------------------------------------------------------------------------

// buildSendRequest converts Matrix message content into a Yunhu send request.
// Media references are resolved through yc so that uploads stored by this
// homeserver can be re-hosted on Yunhu's bucket before the message is sent.
func (s *Server) buildSendRequest(ctx context.Context, yc *yunhu.Client, chatID string, chatType int32, evType string, content map[string]any, msgID string, nameOf ...func(string) string) (*pb.SendMsgRequest, error) {
	req := &pb.SendMsgRequest{
		MsgId:    msgID,
		ChatId:   chatID,
		ChatType: pb.ChatType(chatType),
		Content:  &pb.MsgContent{},
	}

	msgtype := asString(content["msgtype"])
	body := asString(content["body"])
	if evType != "m.room.message" {
		return nil, fmt.Errorf("unsupported event type %q", evType)
	}

	// Strip a Matrix reply's quoted block from the body before anything else
	// looks at it: the quote is metadata (it goes back out as quote_msg_text and
	// quote_msg_id), and leaving it in would deliver the quoted text as part of
	// the new message - both as a "> " prefix in the body and as visible noise in
	// Yunhu's own client.
	var quoteText string
	if rel, ok := content["m.relates_to"].(map[string]any); ok {
		if inReply, ok := rel["m.in_reply_to"].(map[string]any); ok {
			if id := yunhuMsgIDFromEvent(asString(inReply["event_id"])); id != "" {
				req.QuoteMsgId = id
			}
			quoteText, body = splitMatrixReply(body)
		}
	}

	switch msgtype {
	case "m.text", "m.notice", "m.emote", "":
		req.ContentType = pb.ContentType_text
		req.Content.Text = body

	case "m.image":
		req.ContentType = pb.ContentType_image
		ref, err := s.resolveMedia(ctx, yc, content, "image")
		if err != nil {
			return nil, err
		}
		// `image` (9) is "key/url" and is what Yunhu's own client puts here
		// after uploading (it sends the bare Qiniu key and nothing else).
		// `image_url` (3) is the documented field for the JSON API, but the
		// server turns a key into that URL itself - sending a full URL there as
		// well risks a double prefix, so this follows the client exactly.
		req.Content.Image = ref
		// filename, not body: body is the caption (see mediaNote), and using it
		// as the name labelled every screenshot with whatever the sender typed.
		req.Content.FileName = matrixFileName(content, mediaFileName(ref, "image.jpg"))
		mi := parseMediaInfo(content)
		req.Content.Width = mi.Width
		req.Content.Height = mi.Height
		// Yunhu only documents file_size for files, but the field is the one
		// place a size can go and its own client fills it for images too.
		req.Content.FileSize = mi.Size
		req.Media = mediaAttachment(ref, mi.MimeType, "", mi)

	case "m.sticker":
		// Yunhu renders stickers as images; the sticker metadata is lost but the
		// message still arrives intact on the other side.
		req.ContentType = pb.ContentType_image
		ref, err := s.resolveMedia(ctx, yc, content, "image")
		if err != nil {
			return nil, err
		}
		req.Content.Image = ref
		req.Content.FileName = matrixFileName(content, mediaFileName(ref, "sticker.png"))
		mi := parseMediaInfo(content)
		req.Content.Width = mi.Width
		req.Content.Height = mi.Height
		req.Content.FileSize = mi.Size

	case "m.file":
		req.ContentType = pb.ContentType_file
		ref, err := s.resolveMedia(ctx, yc, content, "file")
		if err != nil {
			return nil, err
		}
		// filename, never body: body is the note (see fileNote), and using it as
		// the fallback name renamed the attachment to the caption when a client
		// sent a note without a filename.
		req.Content.FileName = matrixFileName(content, "file.bin")
		req.Content.FileUrl = ref
		mi := parseMediaInfo(content)
		req.Content.FileSize = mi.Size
		req.Media = mediaAttachment(ref, mi.MimeType, "", mi)

	case "m.audio":
		req.ContentType = pb.ContentType_audio
		ref, err := s.resolveMedia(ctx, yc, content, "audio")
		if err != nil {
			return nil, err
		}
		req.Content.AudioUrl = ref
		req.Content.FileName = matrixFileName(content, mediaFileName(ref, "voice.m4a"))
		mi := parseMediaInfo(content)
		// Yunhu counts seconds; Matrix sends milliseconds.
		if mi.Duration > 0 {
			req.Content.AudioTime = int32(math.Round(float64(mi.Duration) / 1000))
		}
		req.Content.FileSize = mi.Size
		req.Media = mediaAttachment(ref, mi.MimeType, "", mi)

	case "m.video":
		// Yunhu has a dedicated video content type (10) - the docs' enum omits
		// it, but Yunhu's own Android client sends CONTENT_TYPE_VIDEO = 10, and
		// without it the receiving client does not treat the message as a video
		// at all: it renders as a text bubble that happens to carry a url.
		req.ContentType = pb.ContentType_video
		ref, err := s.resolveMedia(ctx, yc, content, "video")
		if err != nil {
			return nil, err
		}
		req.Content.VideoUrl = ref
		req.Content.FileName = matrixFileName(content, mediaFileName(ref, "video.mp4"))
		// No caption in text: Yunhu's client renders a type-10 message as a
		// player only and never draws content.text, so anything written there is
		// invisible. The caption goes out as its own message instead - see
		// carryNote / sendEvent.
		mi := parseMediaInfo(content)
		if mi.Duration > 0 {
			req.Content.VideoTime = int32(math.Round(float64(mi.Duration) / 1000))
		}
		req.Content.Width = mi.Width
		req.Content.Height = mi.Height
		req.Content.FileSize = mi.Size
		// The attachment metadata is what makes the receiving client treat this
		// as a playable video rather than an opaque blob.
		req.Media = mediaAttachment(ref, mi.MimeType, "mp4", mi)

	case "m.location":
		req.ContentType = pb.ContentType_text
		req.Content.Text = body

	default:
		req.ContentType = pb.ContentType_text
		req.Content.Text = body
	}

	// Mentions.
	//
	// Both directions have to be rewritten, not just copied: Matrix mentions an
	// <a href> target plus a user_id, while Yunhu stores the target ids in
	// mentioned_id AND expects the body to spell out "@[显示名]" at each spot.
	// Sending a Matrix body untouched gave the other side a literal "@名字" (or
	// worse, a bare name) with no mention pill, and leaving mentioned_id empty
	// meant the mention did not notify anybody.
	if mentionIDs, ok := mentionUserIDsFrom(content); ok && len(mentionIDs) > 0 {
		for _, uid := range mentionIDs {
			if id, _, ok := s.yunhuIDFromUser(uid); ok {
				req.Content.MentionedId = append(req.Content.MentionedId, id)
			}
		}
		// Rewrite the body for the targets that could be named: an id we cannot
		// resolve has no display name, and inventing one would mention the wrong
		// person.
		resolveName := func(userID string) string { return model.LocalpartOf(userID) }
		if len(nameOf) > 0 && nameOf[0] != nil {
			resolveName = nameOf[0]
		}
		if text := rewriteMentionsForYunhu(body, mentionIDs, resolveName); text != "" {
			req.Content.Text = text
		}
	}

	// Replies: the ids and the quote text were resolved at the top, before the
	// body was rendered; only the quote text still has to be attached.
	if quoteText != "" {
		req.Content.QuoteMsgText = quoteText
	}
	return req, nil
}

// splitMatrixReply separates a Matrix reply body into the quoted text and the
// new text.
//
// The fallback body of a reply looks like:
//
//	> <@alice:example.org> the quoted line
//	> and a second line
//
//	the actual message
//
// The quote is every line starting with ">", the blank line ends it, and
// everything after is the new message. A body without that shape is returned
// whole as the new text, so a client that only set the relation is not mangled.
func splitMatrixReply(body string) (quoted, rest string) {
	if !strings.HasPrefix(body, ">") {
		return "", body
	}
	lines := strings.Split(body, "\n")
	i := 0
	var quote []string
	for ; i < len(lines); i++ {
		line := lines[i]
		if strings.HasPrefix(line, ">") {
			quote = append(quote, strings.TrimSpace(strings.TrimPrefix(line, ">")))
			continue
		}
		break
	}
	rest = strings.TrimLeft(strings.Join(lines[i:], "\n"), "\n")

	// The first line names the author as "<@user:server> ", which is Matrix
	// fallback syntax rather than message text: Yunhu shows the real author from
	// quote_msg_id, so the placeholder would be visible noise.
	if len(quote) > 0 {
		first := quote[0]
		if strings.HasPrefix(first, "<@") {
			if i := strings.IndexByte(first, '>'); i >= 0 {
				quote[0] = strings.TrimSpace(first[i+1:])
			}
		}
	}
	return strings.Join(quote, "\n"), rest
}

// mediaInfo reads the m.info object of a Matrix media message.
//
// Matrix puts everything descriptive there: mimetype, size, pixel dimensions,
// duration (milliseconds) and a blurhash. Yunhu has fields for most of it but
// expects different units (duration in seconds) and takes the size only on
// files, so the mapping is done per message type below.
type mediaInfo struct {
	Size     int64
	Width    int32
	Height   int32
	Duration int32 // milliseconds, as Matrix sends it
	MimeType string
}

func parseMediaInfo(content map[string]any) mediaInfo {
	var out mediaInfo
	info, ok := content["info"].(map[string]any)
	if !ok {
		return out
	}
	out.Size = asInt64(info["size"])
	out.Width = int32(asInt(info["w"]))
	out.Height = int32(asInt(info["h"]))
	out.Duration = int32(asInt(info["duration"]))
	out.MimeType = asString(info["mimetype"])
	return out
}

// matrixFileName picks the name to send upstream.
//
// Matrix splits a media message in two: filename is the real file name, body is a
// human-readable description (what clients show as a caption). Yunhu has one
// name field and no caption, so the name must come from filename and the caption
// travels separately - using body here renamed every screenshot to whatever the
// sender typed in the description box.
func matrixFileName(content map[string]any, fallback string) string {
	if name := strings.TrimSpace(asString(content["filename"])); name != "" {
		return name
	}
	// Some clients only fill body. Accept it when it looks like a file name
	// (has an extension) rather than a sentence.
	if body := strings.TrimSpace(asString(content["body"])); body != "" && hasFileExtension(body) {
		return body
	}
	return fallback
}

// hasFileExtension reports whether s ends in a short alphanumeric extension.
func hasFileExtension(s string) bool {
	i := strings.LastIndexByte(s, '.')
	if i <= 0 || i == len(s)-1 {
		return false
	}
	ext := s[i+1:]
	if len(ext) < 1 || len(ext) > 8 {
		return false
	}
	for _, r := range ext {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9':
		default:
			return false
		}
	}
	return true
}

// mediaNote returns the caption of an image/video/audio message, which is what
// Matrix keeps in body when a separate filename is present.
//
// Like a file's note it cannot be carried by Yunhu's media message, so the caller
// sends it as its own text message. It returns "" when body is really the file
// name, which is how many clients (and every message from this homeserver) fill
// both fields - splitting in that case would post the file name twice.
func mediaNote(content map[string]any) string {
	switch asString(content["msgtype"]) {
	case "m.image", "m.video", "m.audio":
	default:
		return ""
	}
	body := strings.TrimSpace(asString(content["body"]))
	if body == "" {
		return ""
	}
	if body == strings.TrimSpace(asString(content["filename"])) {
		return ""
	}
	return body
}

// carryNote reports whether a media message's caption has to be sent as its own
// text message.
//
// Every media type needs this, video included. Yunhu's client renders a
// content_type=10 message as a video player and *nothing else* - it does not
// draw `content.text` at all (MessageContentView.kt: `10 -> VideoMessageView(...)`,
// with no text branch). Putting the caption in that message's text therefore made
// it invisible on the other side, which is exactly how a caption "disappeared".
func carryNote(content map[string]any) string {
	switch asString(content["msgtype"]) {
	case "m.file":
		return fileNote(content)
	case "m.image", "m.sticker", "m.audio", "m.video":
		return mediaNote(content)
	}
	return ""
}

// fileNote returns the note a Matrix file message carries in its body.
//
// A Matrix file message has two text fields: body (a human-readable description,
// which clients show as a caption) and filename (the actual file name). Yunhu
// has one field for the name and no caption at all, so a note attached to the
// file has nowhere to go - it used to be silently dropped, and worse, when the
// body was a note and the filename was absent the note became the file name.
//
// The caller sends such a note as its own message instead.
func fileNote(content map[string]any) string {
	if asString(content["msgtype"]) != "m.file" {
		return ""
	}
	body := strings.TrimSpace(asString(content["body"]))
	name := strings.TrimSpace(asString(content["filename"]))
	if body == "" || body == name {
		// No body, or the body is just the file name repeated: nothing to say.
		return ""
	}
	return body
}

// mediaAttachment builds the SendMsgRequest.media object for a media message.
//
// Yunhu's own client sends this alongside the content key for images, videos and
// voice notes: it carries the same object key plus the MIME type, byte size,
// pixel dimensions and file suffix. The receiving side picks a renderer from it -
// without media, a video arrives as a content bubble the client does not know how
// to play, which is exactly the "解析错误 / 空消息" symptom.
//
// file_key2 must repeat file_key: Yunhu's client fills both and the documented
// behaviour is that omitting the second one fails.
func mediaAttachment(key, mimeType, suffix string, mi mediaInfo) *pb.Media {
	if key == "" || strings.HasPrefix(key, "http://") || strings.HasPrefix(key, "https://") {
		// An absolute URL is not an object key, and media.file_key has to be the
		// key. Sending the URL there would make Yunhu build a nonsense path.
		return nil
	}
	media := &pb.Media{
		FileKey:  key,
		FileKey2: key,
		FileType: mimeType,
	}
	if media.FileType == "" {
		media.FileType = mediaMime(key)
	}
	if media.FileType == "" {
		media.FileType = "application/octet-stream"
	}
	// The suffix comes from the object key, which always ends in one (a key
	// without an extension renders as an unnamed attachment). An explicit one
	// overrides it when the caller knows better.
	suffix = strings.TrimPrefix(strings.ToLower(strings.TrimSpace(suffix)), ".")
	if suffix == "" {
		suffix = strings.TrimPrefix(strings.ToLower(path.Ext(key)), ".")
	}
	media.FileSuffix = suffix
	media.FileSize = mi.Size
	media.ImageWidth = int64(mi.Width)
	media.ImageHeight = int64(mi.Height)
	// The hash is the MD5 of the bytes, which is also what the object key is
	// derived from - so the key's own base name is the hash when this homeserver
	// uploaded it.
	if base := path.Base(key); base != "" && base != key {
		if i := strings.IndexByte(base, '.'); i > 0 {
			media.FileHash = base[:i]
		}
	}
	return media
}

// resolveMedia extracts and resolves the media reference of a message.
func (s *Server) resolveMedia(ctx context.Context, yc *yunhu.Client, content map[string]any, kind string) (string, error) {
	ref := asString(content["url"])
	if ref == "" {
		ref = asString(content["file"])
	}
	if ref == "" {
		return "", fmt.Errorf("message has no media url")
	}
	return s.Media.ResolveForYunhu(ctx, yc, ref, kind)
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

// yunhuIDFromUser reverses mxUserID.
func (s *Server) yunhuIDFromUser(userID string) (id string, chatType int32, ok bool) {
	suffix := ":" + s.Cfg.Server.Name
	if !strings.HasSuffix(userID, suffix) {
		return "", 0, false
	}
	local := strings.TrimSuffix(strings.TrimPrefix(userID, "@"), suffix)
	switch {
	case strings.HasPrefix(local, userPrefixUser):
		return strings.TrimPrefix(local, userPrefixUser), model.ChatTypeUser, true
	case strings.HasPrefix(local, userPrefixBot):
		return strings.TrimPrefix(local, userPrefixBot), model.ChatTypeBot, true
	case strings.HasPrefix(local, userPrefixGroup):
		return strings.TrimPrefix(local, userPrefixGroup), model.ChatTypeGroup, true
	}
	return "", 0, false
}

func asString(v any) string {
	switch t := v.(type) {
	case string:
		return t
	case fmt.Stringer:
		return t.String()
	case float64:
		return strconv.FormatFloat(t, 'f', -1, 64)
	case int:
		return strconv.Itoa(t)
	case int64:
		return strconv.FormatInt(t, 10)
	}
	return ""
}

func asInt(v any) int {
	switch t := v.(type) {
	case float64:
		return int(t)
	case int:
		return t
	case int64:
		return int(t)
	case int32:
		// Message content is map[string]any, so a value built from an int32 (a
		// duration in seconds, a proto field) keeps that type and would otherwise
		// read back as 0.
		return int(t)
	case string:
		n, _ := strconv.Atoi(t)
		return n
	}
	return 0
}

func asInt64(v any) int64 {
	switch t := v.(type) {
	case float64:
		return int64(t)
	case int:
		return int64(t)
	case int64:
		return t
	case int32:
		return int64(t)
	case string:
		n, _ := strconv.ParseInt(t, 10, 64)
		return n
	}
	return 0
}

func asMap(v any) map[string]any {
	if m, ok := v.(map[string]any); ok {
		return m
	}
	return nil
}

func orDefault(v, def string) string {
	if strings.TrimSpace(v) == "" {
		return def
	}
	return v
}

var (
	reMDCode   = regexp.MustCompile("(?s)```(\\w*)\\n?(.*?)```")
	reMDBold   = regexp.MustCompile(`\*\*(.+?)\*\*`)
	reMDItalic = regexp.MustCompile(`(^|[^*])\*([^*]+)\*`)
	reMDInline = regexp.MustCompile("`([^`]+)`")
	reMDLink   = regexp.MustCompile(`\[([^\]]+)\]\((https?://[^)\s]+)\)`)
	reHTMLTag  = regexp.MustCompile(`(?s)<[^>]*>`)
)

// mdToHTML renders the small Markdown subset Yunhu clients actually use.
func mdToHTML(s string) string {
	out := html.EscapeString(s)
	out = reMDCode.ReplaceAllString(out, "<pre><code>$2</code></pre>")
	out = reMDLink.ReplaceAllString(out, `<a href="$2">$1</a>`)
	out = reMDBold.ReplaceAllString(out, "<strong>$1</strong>")
	out = reMDItalic.ReplaceAllString(out, "$1<em>$2</em>")
	out = reMDInline.ReplaceAllString(out, "<code>$1</code>")
	out = strings.ReplaceAll(out, "\n", "<br>")
	return out
}

// stripHTML produces a plain-text fallback body.
func stripHTML(s string) string {
	out := reHTMLTag.ReplaceAllString(s, "")
	out = html.UnescapeString(out)
	return strings.TrimSpace(out)
}
