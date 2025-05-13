package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/IBM/sarama"
	"github.com/jackc/pgx/v5"
	"gopkg.in/yaml.v2"
)

var BrokerSet = "localhost:9092"
var PsqlHostSet = "localhost"
var PsqlUserSet = "myuser"
var PsqlHostPortSet = "5431"
var PsqlPassSet = "mypassword"
var PsqlDBSet = "mydb"
var KafkaConnectSet = "http://localhost:8083"

func main() {
	modeFlag := flag.String("mode", "", "start mode: create | delete | run")
	brokerFlag := flag.String("broker", "", "kafka broker address with port, e.g. localhost:9092")
	psqlHostFlag := flag.String("psql-host", "", "postgresql host address")
	psqlHostPort := flag.String("psql-host-port", "", "postgresql host port")
	psqlUserFlag := flag.String("psql-user", "", "postgresql user")
	psqlPassFlag := flag.String("psql-pass", "", "postgresql password")
	psqlDBFlag := flag.String("psql-db", "", "postgresql database")
	kafkaConnectFlag := flag.String("kafka-connect", "", "kafka connect address with port, e.g. localhost:8083")

	flag.Parse()

	// Ensure mode
	if modeFlag == nil || *modeFlag == "" {
		fmt.Println("No mode specified!")
		flag.PrintDefaults()
		return
	}

	// Set defaults
	if brokerFlag != nil && *brokerFlag != "" {
		BrokerSet = *brokerFlag
	}

	if psqlHostFlag != nil && *psqlHostFlag != "" {
		PsqlHostSet = *psqlHostFlag
	}

	if psqlHostPort != nil && *psqlHostPort != "" {
		PsqlHostPortSet = *psqlHostPort
	}

	if psqlUserFlag != nil && *psqlUserFlag != "" {
		PsqlUserSet = *psqlUserFlag
	}

	if psqlPassFlag != nil && *psqlPassFlag != "" {
		PsqlPassSet = *psqlPassFlag
	}

	if psqlDBFlag != nil && *psqlDBFlag != "" {
		PsqlDBSet = *psqlDBFlag
	}

	if kafkaConnectFlag != nil && *kafkaConnectFlag != "" {
		KafkaConnectSet = *kafkaConnectFlag
	}

	// Fields to test
	insertMode := "upsert" // insert | upsert | update
	pollIntervalMs := (time.Millisecond * 100).Milliseconds()
	switch *modeFlag {
	case "create":
		setupStack(insertMode, pollIntervalMs)
	case "delete":
		teardownStack()
	case "run":
		runTests()
	default:
		fmt.Printf("Unknown mode: %s", *modeFlag)
	}
}

func setupStack(insertMode string, pollIntervalMs int64) {
	// Read topics from file
	dat, err := os.ReadFile("topics.yml")
	if err != nil {
		print(err)
	}

	var topics map[string]interface{}
	err = yaml.Unmarshal(dat, &topics)
	if err != nil {
		print(err)
	}

	topicNames := topics["topics"].([]interface{})

	// Create topics within Kafka
	config := sarama.NewConfig()
	config.Version = sarama.V3_4_1_0
	admin, err := sarama.NewClusterAdmin([]string{BrokerSet}, config)
	if err != nil {
		print(err)
	}

	defer admin.Close()

	for _, eachTopic := range topicNames {
		currentTopic := eachTopic.(string)
		err = admin.CreateTopic(currentTopic, &sarama.TopicDetail{NumPartitions: 1, ReplicationFactor: 1}, false)
		if err != nil {
			print(err)
		}
		fmt.Println("Created topic:", currentTopic)
	}

	// Create tables within Postgres
	conn, err := pgx.Connect(context.Background(), "postgres://"+PsqlUserSet+":"+PsqlPassSet+"@"+PsqlHostSet+":"+PsqlHostPortSet+"/"+PsqlDBSet)
	if err != nil {
		print(err)
	}
	defer conn.Close(context.Background())

	sqlFiles, err := os.ReadDir("sql/")
	if err != nil {
		print(err)
	}

	for _, eachFile := range sqlFiles {
		currentFile := eachFile.Name()
		fmt.Println("Executing SQL script:", currentFile)
		dat, err := os.ReadFile("sql/" + currentFile)
		if err != nil {
			print(err)
		}
		sqlString := string(dat)
		_, err = conn.Exec(context.Background(), sqlString)
		if err != nil {
			print(err)
		}
		fmt.Println("Executed SQL script:", currentFile)
	}

	// Create Kafka Connect connectors
	for _, eachTopic := range topicNames {
		kafkaPayload := map[string]interface{}{
			"name": eachTopic.(string) + "-connector",
			"config": map[string]interface{}{
				"connector.class":                "io.confluent.connect.jdbc.JdbcSinkConnector",
				"tasks.max":                      "1",
				"topics":                         eachTopic.(string),
				"connection.url":                 "jdbc:postgresql://" + "postgres:5432" + "/" + PsqlDBSet,
				"connection.user":                PsqlUserSet,
				"connection.password":            PsqlPassSet,
				"auto.create":                    "false",
				"auto.evolve":                    "false",
				"insert.mode":                    insertMode,
				"table.name.format":              eachTopic.(string) + "_table",
				"value.converter":                "org.apache.kafka.connect.json.JsonConverter",
				"value.converter.schemas.enable": "true",
				"key.converter":                  "org.apache.kafka.connect.json.JsonConverter",
				"key.converter.schemas.enable":   "true",
				"delete.enabled":                 "true",
				"delete.key.fields":              "id",
				"poll.interval.ms":               pollIntervalMs,
			},
		}

		// The following must be set if using upsert
		if insertMode == "upsert" {
			kafkaPayload["config"].(map[string]interface{})["pk.mode"] = "record_key"
			kafkaPayload["config"].(map[string]interface{})["pk.fields"] = "id"
		}

		dat, err := json.Marshal(kafkaPayload)
		if err != nil {
			print(err)
		}

		resp, err := http.Post(KafkaConnectSet+"/connectors", "application/json", bytes.NewBuffer(dat))
		if err != nil {
			print(err)
		}
		defer resp.Body.Close()
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			print(err)
		}
		if resp.StatusCode != 201 {
			print("Failed to create Kafka Connect connector: " + string(body) + ", status code: " + strconv.Itoa(resp.StatusCode))
		}
		fmt.Println("Created Kafka Connect connector: ", eachTopic.(string)+"-connector")
	}
}

func teardownStack() {
	// Delete topics within Kafka
	dat, err := os.ReadFile("topics.yml")
	if err != nil {
		print(err)
	}

	var topics map[string]interface{}
	err = yaml.Unmarshal(dat, &topics)
	if err != nil {
		print(err)
	}

	topicNames := topics["topics"].([]interface{})

	// Delete Kafka Connect connectors
	for _, eachTopic := range topicNames {
		connectorName := eachTopic.(string) + "-connector"
		// resp, err := http.MethodDelete(KafkaConnectSet + "/connectors/" + connectorName)
		req, err := http.NewRequest("DELETE", KafkaConnectSet+"/connectors/"+connectorName, nil)
		if err != nil {
			print(err)
		}

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			print(err)
		}

		defer resp.Body.Close()
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			print(err)
		}
		if resp.StatusCode != 204 {
			print("Failed to delete Kafka Connect connector: " + string(body) + ", status code: " + strconv.Itoa(resp.StatusCode))
		}
		fmt.Println("Deleted Kafka Connect connector:", connectorName)
	}

	// Create topics within Kafka
	config := sarama.NewConfig()
	config.Version = sarama.V3_4_1_0
	admin, err := sarama.NewClusterAdmin([]string{BrokerSet}, config)
	if err != nil {
		print(err)
	}

	defer admin.Close()

	for _, eachTopic := range topicNames {
		currentTopic := eachTopic.(string)
		// err = admin.CreateTopic(currentTopic, &sarama.TopicDetail{NumPartitions: 1, ReplicationFactor: 1}, false)
		err = admin.DeleteTopic(currentTopic)
		if err != nil {
			print(err)
		}
		fmt.Println("Deleted topic:", currentTopic)
	}

	// Delete tables within Postgres
	conn, err := pgx.Connect(context.Background(), "postgres://"+PsqlUserSet+":"+PsqlPassSet+"@"+PsqlHostSet+":"+PsqlHostPortSet+"/"+PsqlDBSet)
	if err != nil {
		print(err)
	}
	defer conn.Close(context.Background())

	rows, err := conn.Query(context.Background(), "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';")
	if err != nil {
		print(err)
	}
	tablesToDrop := []string{}
	for rows.Next() {
		var tableName string
		err = rows.Scan(&tableName)
		if err != nil {
			print(err)
		}
		fmt.Println("Discovered table: ", tableName)
		tablesToDrop = append(tablesToDrop, tableName)
	}
	rows.Close()

	for _, eachTable := range tablesToDrop {
		_, err = conn.Exec(context.Background(), "DROP TABLE "+eachTable+" CASCADE;")
		if err != nil {
			print(err)
		}
		fmt.Println("Dropped table:", eachTable)
	}

}

func runTests() {
	conn, err := pgx.Connect(context.Background(), "postgres://"+PsqlUserSet+":"+PsqlPassSet+"@"+PsqlHostSet+":"+PsqlHostPortSet+"/"+PsqlDBSet)
	if err != nil {
		print(err)
	}
	defer conn.Close(context.Background())

	payload := map[string]interface{}{
		"schema": map[string]interface{}{
			"type": "struct",
			"fields": []map[string]string{
				map[string]string{
					"field":    "value",
					"type":     "string",
					"optional": "false",
				},
			},
		},
		"payload": map[string]string{
			"value": "random_value",
		},
	}

	key := map[string]interface{}{
		"schema": map[string]interface{}{
			"type": "struct",
			"fields": []map[string]string{
				map[string]string{
					"field":    "id",
					"type":     "string",
					"optional": "false",
				},
			},
		},
		"payload": map[string]string{
			"id": "random_id",
		},
	}

	keyBytes, err := json.Marshal(key)
	if err != nil {
		print(err)
	}

	producer, err := NewKafkaProducer("localhost:9092", "test_transaction")
	if err != nil {
		print(err)
	}
	defer producer.Close() // Commit and close the producer at the end of the program

	// Produce message
	err = producer.Produce("test_topic", keyBytes, payload)
	if err != nil {
		print(err)
	}
	err = producer.Commit()
	if err != nil {
		print(err)
	}

	// Query Psql for result
	startTime := time.Now()
	for {
		var id string
		err = conn.QueryRow(context.Background(), "SELECT id FROM test_topic_table LIMIT 1;").Scan(&id)
		if err == nil {
			fmt.Println("Record Found!")
			break
		}
		if err == pgx.ErrNoRows {
			fmt.Print(".")
		}

		if time.Since(startTime) > time.Second*3 {
			break
		}
	}
	elapsedTime := time.Since(startTime)
	fmt.Println("Query took", elapsedTime)
	file, err := os.OpenFile("data.txt", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		fmt.Println("Error opening file:", err)
		return
	}
	defer file.Close()

	_, err = fmt.Fprintf(file, "%d\n", elapsedTime.Nanoseconds())
	if err != nil {
		fmt.Println("Error writing to file:", err)
		return
	}

}
