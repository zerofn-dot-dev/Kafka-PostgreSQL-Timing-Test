package main

import (
	"encoding/json"
	"time"

	"github.com/IBM/sarama"
)

type KafkaProducer struct {
	Broker        string
	TransactionId string

	producer *sarama.AsyncProducer
}

func NewKafkaProducer(broker, transactionId string) (*KafkaProducer, error) {
	cfg := sarama.NewConfig()
	cfg.ClientID = transactionId
	cfg.Version = sarama.V3_4_0_0
	cfg.Producer.Return.Successes = false
	cfg.Producer.Return.Errors = false
	cfg.Producer.Idempotent = true
	cfg.Producer.Compression = sarama.CompressionZSTD
	cfg.Producer.Retry.Max = 30
	cfg.Producer.Retry.Backoff = time.Millisecond * 10
	cfg.Metadata.AllowAutoTopicCreation = false
	cfg.Producer.RequiredAcks = sarama.WaitForAll
	cfg.Net.MaxOpenRequests = 1
	cfg.Producer.Transaction.ID = transactionId

	newProducer, err := sarama.NewAsyncProducer([]string{broker}, cfg)
	if err != nil {
		return nil, err
	}

	err = newProducer.BeginTxn()
	if err != nil {
		return nil, err
	}

	return &KafkaProducer{
		Broker:        broker,
		TransactionId: transactionId,
		producer:      &newProducer,
	}, nil
}

func (producer *KafkaProducer) Produce(topic string, key []byte, message interface{}) error {
	messageBytes, err := json.Marshal(message)
	if err != nil {
		return err
	}

	var valReturn sarama.ByteEncoder = nil
	if message != nil {
		valReturn = sarama.ByteEncoder(messageBytes)
	}

	msg := &sarama.ProducerMessage{
		Topic: topic,
		Key:   sarama.ByteEncoder(key),
		Value: valReturn,
	}

	(*producer.producer).Input() <- msg
	return nil
}

func (producer *KafkaProducer) Commit() error {
	return (*producer.producer).CommitTxn()
}

func (producer *KafkaProducer) Close() {
	(*producer.producer).Close()
}

func (producer *KafkaProducer) CommitAndClose() {
	(*producer.producer).CommitTxn()
	(*producer.producer).Close()
}
