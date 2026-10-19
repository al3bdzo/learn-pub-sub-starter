package pubsub


import (
	amqp "github.com/rabbitmq/amqp091-go"
	"encoding/json"
	"encoding/gob"
	"context"
	"bytes"
)

func PublishJSON[T any](ch *amqp.Channel, exchange, key string, val T) error {
	jsonData, err := json.Marshal(val)
	if err != nil {
		return err
	}

	err = ch.PublishWithContext(context.Background(), exchange, key, false, false, amqp.Publishing{
		ContentType: "application/json",
		Body: jsonData,
	})
	if err != nil {
		return err
	}

	return nil
}


func PublishGOB[T any](ch *amqp.Channel, exchange, key string, val T) error {
	var data bytes.Buffer
	encoder := gob.NewEncoder(&data)
	err := encoder.Encode(val)
	if err != nil {
		return err
	}

	err = ch.PublishWithContext(context.Background(), exchange, key, false, false, amqp.Publishing{
		ContentType: "application/gob",
		Body: data.Bytes(),
	})
	if err != nil {
		return err
	}

	return nil
}