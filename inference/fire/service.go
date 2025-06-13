package fire

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	"github.com/IBM/sarama"
	"github.com/gorilla/mux"
	"github.com/lovoo/goka"
)

// View 생성
func Run(brokers []string, stream goka.Stream, cfg *sarama.Config) {
	// View 생성
	view, err := goka.NewView(brokers, FireStateTable, new(FireStateCodec))
	if err != nil {
		panic(err)
	}
	go view.Run(context.Background())

	// Router 생성
	router := mux.NewRouter()
	router.HandleFunc("/fireDetection/{sensorID}/feed", feed(view)).Methods("GET")

	log.Printf("Listen port 8080")
	log.Fatal(http.ListenAndServe(":8080", router))
}

// View 사용하여 메세지 피드 HTTP로 출력(핸들러 함수 return)
func feed(view *goka.View) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		sensorID := mux.Vars(r)["sensorID"]
		val, _ := view.Get(sensorID)
		if val == nil {
			fmt.Fprintf(w, "%s not found!", sensorID)
			return
		}
		messages := val.(FireState)
		fmt.Fprintf(w, "messages: %v\n", messages)
	}
}

// View 사용하여 메세지 피드 HTTP로 출력(핸들러 함수 return)
func lastFireDetection(view *goka.View) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		sensorID := mux.Vars(r)["sensorID"]
		val, err := view.Get(sensorID)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(map[string]string{"error": "Failed to get sensor data"})
			return
		}

		if val == nil {
			w.WriteHeader(http.StatusNotFound)
			json.NewEncoder(w).Encode(map[string]string{"error": fmt.Sprintf("Sensor %s not found", sensorID)})
			return
		}

		messages := val.(FireState)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"sensor_id":            sensorID,
			"last_fire_detection":  messages.LastFireDetection,
			"fire_detection_count": messages.FireDetectionCount,
			"recent_fire_messages": messages.RecentFireMessages,
		})
	}
}
