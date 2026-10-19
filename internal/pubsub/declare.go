package pubsub

import (
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
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