package yunhu

import "strings"

// Yunhu does not return absolute URLs for everything: stickers, expressions and
// upload keys come back as bare object keys ("sticker/85ade....gif"). The API
// reference documents which CDN serves which kind of object:
//
//	图片路由: https://chat-img.jwznb.com/ (+ chat-img2 / chat-img3)
//	音频路由: https://chat-audio1.jwznb.com/
//	文件路由: https://chat-file.jwznb.com/ (+ chat-storage1 / chat-file-oss)
//	视频路由: https://chat-video1.jwznb.com/
//
// and says outright, for stickers and expressions:
//
//	表情包 URL, 需要前面加上 https://chat-img.jwznb.com/
//
// A single flat base cannot express that: the old default (a Qiniu origin host)
// stopped resolving, and pointing everything at one host breaks audio and files.
const (
	// MediaHostImage serves images, stickers, expressions and avatars.
	MediaHostImage = "https://chat-img.jwznb.com/"
	// MediaHostAudio serves voice notes and other audio.
	MediaHostAudio = "https://chat-audio1.jwznb.com/"
	// MediaHostVideo serves video attachments.
	MediaHostVideo = "https://chat-video1.jwznb.com/"
	// MediaHostFile serves documents and knowledge-base blobs.
	MediaHostFile = "https://chat-file.jwznb.com/"
)

// mediaKeyRoutes maps the leading path segment of a bare object key onto the
// host that serves it. Longer prefixes win, so "defalut-avatars/" (Yunhu's own
// spelling, kept verbatim) is listed before nothing at all.
var mediaKeyRoutes = []struct{ prefix, host string }{
	{"sticker/", MediaHostImage},
	{"expression/", MediaHostImage},
	{"defalut-avatars/", MediaHostImage},
	{"image/", MediaHostImage},
	{"avatar/", MediaHostImage},
	{"audio/", MediaHostAudio},
	{"voice/", MediaHostAudio},
	{"video/", MediaHostVideo},
	{"file/", MediaHostFile},
	{"knowledge/", MediaHostFile},
}

// MediaURL resolves a Yunhu media reference to a fetchable URL.
//
// Absolute URLs pass through untouched (most message payloads carry them), and a
// bare key is routed to its documented host. Anything unrecognised falls back to
// the image host: that is where the bulk of bare keys live (stickers, avatars,
// backgrounds such as "background9.webp"), and it is the only host that serves
// arbitrary hash-named objects.
func MediaURL(ref string) string {
	ref = strings.TrimSpace(ref)
	if ref == "" {
		return ""
	}
	if strings.HasPrefix(ref, "http://") || strings.HasPrefix(ref, "https://") {
		return ref
	}
	// Keys are stored without a leading slash, but tolerate one.
	key := strings.TrimLeft(ref, "/")
	lower := strings.ToLower(key)
	for _, r := range mediaKeyRoutes {
		if strings.HasPrefix(lower, r.prefix) {
			return r.host + key
		}
	}
	return MediaHostImage + key
}
