// Erik Oomen, jul 2019
//
// Receives streaming event on topic 'sensors' and applies some basic
// business rules to generate a warning or an error on the 'business' topic.
// The rules are:
//   * Readable display (horizontal): Button press -> warning
//   * Turn the cube so the button is on the bottom: Button press -> error.
//
// Warning, not written for stability or performance.

package main

import (
    "context"
    "fmt"
    "log"
    "time"
    "encoding/json"
    kafka "github.com/segmentio/kafka-go"
)

const (
	KAFKA_SERVER    = "192.168.1.106:9092"
	SENSOR_TOPIC    = "sensors"
	BUSINESS_TOPIC  = "business"
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

func getKafkaWriter(kafkaURL, topic string) *kafka.Writer {
        return kafka.NewWriter(kafka.WriterConfig{
                Brokers:  []string{kafkaURL},
                Topic:    topic,
                Balancer: &kafka.LeastBytes{},
                BatchTimeout: 50 * time.Millisecond,
        })
}

func main() {
    kafkaReader := getKafkaReader(KAFKA_SERVER, SENSOR_TOPIC, "")
    defer kafkaReader.Close()

    kafkaWriter := getKafkaWriter(KAFKA_SERVER, BUSINESS_TOPIC)
    defer kafkaWriter.Close()

	fmt.Println("start consuming ... !!")
	for {
                var result map[string]interface{}
		m, err := kafkaReader.ReadMessage(context.Background())
		if err != nil {
			log.Fatalln(err)
		}
                json.Unmarshal([]byte(m.Value),  &result)
		// 
		// Values emperical determined
		// 
		if int(result["button"].(float64)) == 1 {
			level := "0"
			if result["x"].(float64) < -16000.0 {
			    level = "1"
			    fmt.Printf("Severe!\n")
		        }
			business_map := map[string]string{"event_type": "dection", "Device_ID": result["Device_ID"].(string), "Severity": level,
					      "timestamp": m.Time.Format(time.RFC3339)}
                        business_val, _ := json.Marshal(business_map)
			msg := kafka.Message{ Key: []byte(fmt.Sprintf("device-%s", result["Device_ID"])),
					      Value: business_val,
			}
			kafkaWriter.WriteMessages(context.Background(), msg);
			fmt.Printf("Yeah detection @ %v\n", m.Time.Format("Jan _2 15:04:05 2006"));
		}
	}
}

