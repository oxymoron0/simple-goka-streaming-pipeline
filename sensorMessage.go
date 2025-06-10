// 1차 목표: OriginFireTopic 에서 메시지를 받아서 Key 값을 SensorID로 하여 FireStream 으로 전송
package message

import (
	"encoding/json"
	"time"

	"github.com/lovoo/goka"
)

var (
	OriginFireTopic goka.Stream = "fire-detection-event"
	FireStream      goka.Stream = "fire-detection-stream"
)

type OriginMessage struct {
	MessageID        string          `json:"message_id"`
	MessageTimestamp time.Time       `json:"message_timestamp"`
	SourceTimestamp  time.Time       `json:"source_timestamp"`
	SourceWidth      int             `json:"source_width"`
	SourceHeight     int             `json:"source_height"`
	InferenceType    string          `json:"inference_type"`
	InferenceResult  InferenceResult `json:"inference_result"`
}

type InferenceResult struct {
	ID            string          `json:"id"`
	DetectionInfo []DetectionInfo `json:"detectionInfo"`
}

type DetectionInfo struct {
	ClassID    int     `json:"classID"`
	ObjectID   int     `json:"objectID"`
	Left       float64 `json:"left"`
	Top        float64 `json:"top"`
	Width      float64 `json:"width"`
	Height     float64 `json:"height"`
	Confidence float64 `json:"confidence"`
}

// ------------------------------------------------------------

type FireMessage struct {
	SensorID        string          //Sensor ID
	MessageMetadata MessageMetadata // Message Metadata
	Fire            []FireInfo      // Fire Info
}

type MessageMetadata struct {
	MessageID        string
	MessageTimestamp time.Time
	SourceTimestamp  time.Time
	SourceWidth      int
	SourceHeight     int
}

type BBox struct {
	X      float64 `json:"x"` // left
	Y      float64 `json:"y"` // top
	Width  float64 `json:"width"`
	Height float64 `json:"height"`
}

type FireInfo struct {
	ObjectID   int     `json:"object_id"`
	BBox       BBox    // Bounding Box
	Confidence float64 `json:"confidence"`
}

type JSONMessageCodec struct{}

func (c *JSONMessageCodec) Encode(data interface{}) ([]byte, error) {
	return json.Marshal(data)
}

func (c *JSONMessageCodec) Decode(data []byte) (interface{}, error) {
	var originMessage OriginMessage
	err := json.Unmarshal(data, &originMessage)
	return originMessage, err
}
