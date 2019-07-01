package main

import (
    "context"
    "fmt"
    "log"
    "encoding/json"
    kafka "github.com/segmentio/kafka-go"
)

func getKafkaReader(kafkaURL, topic, groupID string) *kafka.Reader {
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{kafkaURL},
		GroupID:  groupID,
		Topic:    topic,
		MinBytes: 10e3, // 10KB
		MaxBytes: 10e6, // 10MB
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
		fmt.Printf("msg: ", m.Time)
		fmt.Printf("message at topic:%v partition:%v offset:%v	%s = %s\n", m.Topic, m.Partition, m.Offset, string(m.Key), string(m.Value))
                json.Unmarshal([]byte(m.Value),  &result)
		fmt.Printf("message: %v\n", result["button"])

	}
}

