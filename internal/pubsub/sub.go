package pubsub

import (
	amqp "github.com/rabbitmq/amqp091-go"

	"encoding/json"
	"encoding/gob"
	"fmt"
	"bytes"
)

type SimpleQueueType int

const (
	SimpleQueueDurable SimpleQueueType = iota
	SimpleQueueTransient
)

type Acktype int

const (
	Ack Acktype = iota
	NackRequeue
	NackDiscard
)

func Subscribe[T any](
	conn *amqp.Connection,
	exchange, 
	queueName,
	key string,
	queueType SimpleQueueType,
	handler func(T) Acktype,
	unmarshaller func([]byte) (T, error),
) error {
	ch, queue, err := DeclareAndBind(conn, exchange, queueName, key, queueType)
	if err != nil {
		return err
	}

	deliveries, err := ch.Consume(queue.Name, "", false, false, false, false, nil)
	if err != nil {
		return err
	}

	go func() {
		defer ch.Close()
		for delivery := range deliveries {
			message, err := unmarshaller(delivery.Body)
			if err != nil {
				_ = delivery.Nack(false, false)
				fmt.Printf("could not unmarshal message: %v\n", err)
				continue
			}

			switch handler(message) {
			case Ack:
				delivery.Ack(false)
				fmt.Println("Ack")
			case NackDiscard:
				delivery.Nack(false, false)
				fmt.Println("NackDiscard")
			case NackRequeue:
				delivery.Nack(false, true)
				fmt.Println("NackRequeue")
			}
			
		}
	}()

	return nil 
}


func SubscribeJSON[T any](
	conn *amqp.Connection,
	exchange, 
	queueName,
	key string,
	queueType SimpleQueueType,
	handler func(T) Acktype,
) error {
	return Subscribe(conn, exchange, queueName, key, queueType, handler, unmarshalJSON)
}

func SubscribeGOB[T any](
	conn *amqp.Connection,
	exchange, 
	queueName,
	key string,
	queueType SimpleQueueType,
	handler func(T) Acktype,
) error {
	return Subscribe(conn, exchange, queueName, key, queueType, handler, unmarshalGOB)
}

func unmarshalJSON[T any](data []byte) (T, error){
	var message T
	err := json.Unmarshal(data, &message)
	return message, err
}

func unmarshalGOB[T any](data []byte) (T, error){
	buffer := bytes.NewBuffer(data)
	decoder := gob.NewDecoder(buffer)
	var message T
	err := decoder.Decode(&message)
	return message, err
}