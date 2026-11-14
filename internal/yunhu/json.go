package yunhu

import (
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// protoJSONUnmarshal decodes JSON into a protobuf message, tolerating unknown
// fields (Yunhu adds them without notice).
func protoJSONUnmarshal(data []byte, m proto.Message) error {
	return protojson.UnmarshalOptions{DiscardUnknown: true}.Unmarshal(data, m)
}
