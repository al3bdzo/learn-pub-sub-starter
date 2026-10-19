package pubsub

import (
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"

	"encoding/json"
	"fmt"
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

func DeclareAndBind(
	conn *amqp.Connection, 
	exchange,
	queueName, 
	key string,
	queueType SimpleQueueType,
) (*amqp.Channel, amqp.Queue, error) {
	ch, err := conn.Channel()
	if err != nil {
		return nil, amqp.Queue{}, err
	}

	table := amqp.NewConnectionProperties()
	table["x-dead-letter-exchange"] = routing.ExchangeDeadLetter
	queue, err := ch.QueueDeclare(
		queueName,
		queueType == SimpleQueueDurable, 
		queueType != SimpleQueueDurable,
		queueType != SimpleQueueDurable,
		false, 
		table,
	)
	if err != nil {
		return nil, amqp.Queue{}, err
	}

	err = ch.QueueBind(queueName, key, exchange, false, nil)
	if err != nil {
		return nil, amqp.Queue{}, err
	}

	return ch, queue, nil
}


func SubscribeJSON[T any](
	conn *amqp.Connection,
	exchange, 
	queueName,
	key string,
	queueType SimpleQueueType,
	handler func(T) Acktype,
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
			var message T

			err := json.Unmarshal(delivery.Body, &message)
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