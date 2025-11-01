package pubsub

import (
	"bytes"
	"context"
	"encoding/gob"
	"encoding/json"

	amqp "github.com/rabbitmq/amqp091-go"
)

// PublishJSON publishes json message using the exchange and the key
func PublishJSON[T any](ch *amqp.Channel, exchange, key string, val T) error {
	jsonVal, err := json.Marshal(val)
	if err != nil {
		return err
	}

	return ch.PublishWithContext(
		context.Background(),
		exchange, key, false,
		false, amqp.Publishing{
			ContentType: "application/json",
			Body:        jsonVal,
		})
}

func PublishGob[T any](ch *amqp.Channel, exchange, key string, val T) error {
	var buf bytes.Buffer
	encoder := gob.NewEncoder(&buf)
	if err := encoder.Encode(val); err != nil {
		return err
	}

	return ch.PublishWithContext(
		context.Background(),
		exchange, key, false,
		false, amqp.Publishing{
			ContentType: "application/gob",
			Body:        buf.Bytes(),
		})
}
