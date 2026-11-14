package matrix

import (
	"encoding/base64"
	"strings"

	"github.com/yunhu/yhmatrix/internal/model"
)

const (
	userPrefixUser  = "yh_"
	userPrefixBot   = "yhbot_"
	userPrefixGroup = "yhgrp_"

	roomPrefixUser  = "!yh-user-"
	roomPrefixGroup = "!yh-group-"
	roomPrefixBot   = "!yh-bot-"

	eventPrefixRaw = "$yh_"
	eventPrefixB64 = "$yhx_"
)

// mxUserID builds the Matrix user ID for a Yunhu identity.
func (s *Server) mxUserID(chatID string, chatType int32) string {
	switch chatType {
	case model.ChatTypeBot:
		return "@" + userPrefixBot + sanitize(chatID) + ":" + s.Cfg.Server.Name
	case model.ChatTypeGroup:
		return "@" + userPrefixGroup + sanitize(chatID) + ":" + s.Cfg.Server.Name
	default:
		return "@" + userPrefixUser + sanitize(chatID) + ":" + s.Cfg.Server.Name
	}
}

// accountUserID builds the Matrix user ID of the logged-in account itself.
func (s *Server) accountUserID(yunhuID string) string {
	return "@" + userPrefixUser + sanitize(yunhuID) + ":" + s.Cfg.Server.Name
}

// roomIDFor derives a stable room ID from a Yunhu conversation.
func (s *Server) roomIDFor(chatID string, chatType int32) string {
	prefix := roomPrefixUser
	switch chatType {
	case model.ChatTypeGroup:
		prefix = roomPrefixGroup
	case model.ChatTypeBot:
		prefix = roomPrefixBot
	}
	return prefix + sanitize(chatID) + ":" + s.Cfg.Server.Name
}

// parseRoomID reverses roomIDFor.
func (s *Server) parseRoomID(roomID string) (chatID string, chatType int32, ok bool) {
	if !strings.HasSuffix(roomID, ":"+s.Cfg.Server.Name) {
		return "", 0, false
	}
	local := strings.TrimSuffix(roomID, ":"+s.Cfg.Server.Name)
	switch {
	case strings.HasPrefix(local, roomPrefixUser):
		return strings.TrimPrefix(local, roomPrefixUser), model.ChatTypeUser, true
	case strings.HasPrefix(local, roomPrefixGroup):
		return strings.TrimPrefix(local, roomPrefixGroup), model.ChatTypeGroup, true
	case strings.HasPrefix(local, roomPrefixBot):
		return strings.TrimPrefix(local, roomPrefixBot), model.ChatTypeBot, true
	}
	return "", 0, false
}

// eventIDFor derives a Matrix event ID from a Yunhu message ID.
func eventIDFor(msgID string) string {
	if msgID == "" {
		return eventPrefixB64 + base64.RawURLEncoding.EncodeToString([]byte("empty"))
	}
	if isEventSafe(msgID) {
		return eventPrefixRaw + msgID
	}
	return eventPrefixB64 + base64.RawURLEncoding.EncodeToString([]byte(msgID))
}

// yunhuMsgIDFromEvent reverses eventIDFor.
func yunhuMsgIDFromEvent(eventID string) string {
	switch {
	case strings.HasPrefix(eventID, eventPrefixRaw):
		return strings.TrimPrefix(eventID, eventPrefixRaw)
	case strings.HasPrefix(eventID, eventPrefixB64):
		raw, err := base64.RawURLEncoding.DecodeString(strings.TrimPrefix(eventID, eventPrefixB64))
		if err != nil {
			return ""
		}
		return string(raw)
	}
	return ""
}

func isEventSafe(s string) bool {
	if len(s) > 200 {
		return false
	}
	for _, r := range s {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '_', r == '-':
		default:
			return false
		}
	}
	return true
}

// sanitize keeps characters that are valid in Matrix localparts and room IDs.
func sanitize(s string) string {
	var b strings.Builder
	for _, r := range s {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '_', r == '-', r == '.':
			b.WriteRune(r)
		default:
			b.WriteByte('_')
		}
	}
	if b.Len() == 0 {
		return "unknown"
	}
	return b.String()
}
