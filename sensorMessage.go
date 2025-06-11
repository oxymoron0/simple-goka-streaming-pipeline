// 1차 목표: OriginFireTopic 에서 메시지를 받아서 Key 값을 SensorID로 하여 FireStream 으로 전송
package messaging

import (
	"encoding/json"
	"time"

	"github.com/lovoo/goka"
)

// Origin Message 는 추후 모든 message를 받고 처리하는 형식 예정
var (
	OriginTopic          goka.Stream = "fire-detection-event" // 추후 이름: Inference-Event
	ValidInferenceStream goka.Stream = "valid-inference-stream"
	FireDetectionStream  goka.Stream = "fire-detection-stream"
)

// 먼저 InferenceType을 통해 어떤 타입의 메시지인지 판단하고 그에 따라 처리하는 형식 예정
// 현재는 Fire 메시지만 처리
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
	ID            string      `json:"id"`
	DetectionInfo interface{} `json:"detectionInfo"`
}

// 즉 Detection Info의 형태는 interface{} 처럼 어떤 값이 들어올지 모르는 상태가 될 수 있음
// var result interface{}
// err := json.Unmarshal(data, &result)
// 사용하여 동적 JSON 파싱
type DetectionInfo struct {
	ClassID    int     `json:"classID"`
	ObjectID   int     `json:"objectID"`
	Left       float64 `json:"left"`
	Top        float64 `json:"top"`
	Width      float64 `json:"width"`
	Height     float64 `json:"height"`
	Confidence float64 `json:"confidence"`
}

type OriginMessageCodec struct{}

func (c *OriginMessageCodec) Encode(data interface{}) ([]byte, error) {
	return json.Marshal(data)
}

func (c *OriginMessageCodec) Decode(data []byte) (interface{}, error) {
	var originMessage OriginMessage
	err := json.Unmarshal(data, &originMessage)
	return originMessage, err
}

// ------------------------------------------------------------

// 특정 메세지 type을 하드코딩으로 추가하는 방식으로 구현 중
// Inference 형태를 추가할 때 자동화하는 작업이 추후 반영되어야 함
type FireMessage struct {
	SensorID        string          // Origin Message의 InferenceResult.ID
	MessageMetadata MessageMetadata // Origin Message의 MessageMetadata
	Fire            []FireInfo      // Origin Message의 DetectionInfo를 FireInfo로 변환한 결과
}

type MessageMetadata struct {
	MessageID        string    // Origin Message의 MessageID, MongoDB에 사용
	MessageTimestamp time.Time // Origin Message의 MessageTimestamp
	SourceTimestamp  time.Time // Origin Message의 SourceTimestamp
	SourceWidth      int       // Origin Message의 SourceWidth
	SourceHeight     int       // Origin Message의 SourceHeight
}

type FireInfo struct {
	ObjectID   int     `json:"object_id"` // Origin Message의 DetectionInfo Object의 ObjectID
	Smoking    bool    `json:"smoking"`   // Origin Message의 DetectionInfo Object의 classID가 1이면 연기(true), 0이면 불(false)
	BBox       BBox    // Origin Message의 DetectionInfo Object의 Bounding Box
	Confidence float64 `json:"confidence"` // Origin Message의 DetectionInfo Object의 Confidence
}

type BBox struct {
	X      float64 `json:"x"`      // Origin Message의 DetectionInfo Object의 left
	Y      float64 `json:"y"`      // Origin Message의 DetectionInfo Object의 top
	Width  float64 `json:"width"`  // Origin Message의 DetectionInfo Object의 width
	Height float64 `json:"height"` // Origin Message의 DetectionInfo Object의 height
}

type FireMessageCodec struct{}

func (c *FireMessageCodec) Encode(data interface{}) ([]byte, error) {
	return json.Marshal(data)
}

func (c *FireMessageCodec) Decode(data []byte) (interface{}, error) {
	var fireMessage FireMessage
	err := json.Unmarshal(data, &fireMessage)
	return fireMessage, err
}
