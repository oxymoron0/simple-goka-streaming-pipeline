package services

import (
	"context"
	"fmt"
	"log"
	"net/http"

	"github.com/gorilla/mux"
	"github.com/lovoo/goka"
	"github.com/lovoo/goka/examples/3-messaging/collector"
	messaging "github.com/oxymoron0/simple-goka-streaming-pipeline"
)
// View 사용하여 메세지 피드 HTTP로 출력(핸들러 함수 return)
func feed(view *goka.View) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		// 동적 URL parameter 추출, HTTP 요청에서 {user} 값을 가져옵니다.
		user := mux.Vars(r)["user"]
		// 추출한 URL parameter(string)를 key 삼아 view의 value 추출
		val, _ := view.Get(user)
		if val == nil {
			fmt.Fprintf(w, "%s not found!", user)
			return
		}
		messages := val.([]messaging.OriginMessage)
		fmt.Fprintf(w, "Latest messages for %s\n", user)
		for i, m := range messages {
			fmt.Fprintf(w, "%d %10s: %v\n", i, m.MessageID, m.InferenceResult)
		}
	}
}
