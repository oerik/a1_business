package main

import (
    "context"
    "fmt"
    "log"
    "time"
    "encoding/json"
    kafka "github.com/segmentio/kafka-go"
)

func getKafkaReader(kafkaURL, topic, groupID string) *kafka.Reader {
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{kafkaURL},
		GroupID:  groupID,
		Topic:    topic,
		MinBytes: 51, // 10KB
		MaxBytes: 10e6, // 10MB
		MaxWait: 100 * time.Millisecond,
	})
}

func main() {
    kafkaReader := getKafkaReader("192.168.1.106:9092", "sensors", "")

    defer kafkaReader.Close()

	fmt.Println("start consuming ... !!")
	for {
                var result map[string]interface{}
		m, err := kafkaReader.ReadMessage(context.Background())
		if err != nil {
			log.Fatalln(err)
		}
                json.Unmarshal([]byte(m.Value),  &result)
		if int(result["button"].(float64)) == 1 {
			fmt.Printf("Yeah detection @ %v\n", m.Time.Format("Jan _2 15:04:05 2006"));
		}
	}
}

