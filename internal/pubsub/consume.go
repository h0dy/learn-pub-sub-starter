package pubsub

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

type SimpleQueueType int

const (
	Durable SimpleQueueType = iota
	Transient
)

type AckType int

const (
	Ack AckType = iota
	NackRequeue
	NackDiscard
)

// DeclareAndBind declares a queue and bind it to the exchange
func DeclareAndBind(
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType, // an enum to represent "durable" or "transient"
) (*amqp.Channel, amqp.Queue, error) {
	rabChan, err := conn.Channel()
	if err != nil {
		return nil, amqp.Queue{}, fmt.Errorf("couldn't create a channel: %v", err)
	}

	// declaring a new queue
	rabQue, err := rabChan.QueueDeclare(
		queueName,            // name
		queueType == Durable, // durable
		queueType != Durable, // delete when unused
		queueType != Durable, // exclusive
		false,                // no-wait
		amqp.Table{ // args
			"x-dead-letter-exchange": "peril_dlx",
		},
	)
	if err != nil {
		return nil, amqp.Queue{}, fmt.Errorf("couldn't declare a queue: %v", err)
	}

	// binding the queue to the exchange
	if err = rabChan.QueueBind(
		rabQue.Name, // queue name
		key,         // routing key
		exchange,    // exchange
		false,       // no-wait
		nil,         // args
	); err != nil {
		return nil, amqp.Queue{}, err
	}

	return rabChan, rabQue, nil
}

func subscribe[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType,
	handler func(T) AckType,
	unmarshaller func([]byte) (T, error),
) error {
	// bind the queue
	ch, qu, err := DeclareAndBind(conn, exchange, queueName, key, queueType)
	if err != nil {
		return fmt.Errorf("couldn't declare and bind queue: %v", err)
	}

	if err := ch.Qos(10, 0, false); err != nil {
		return fmt.Errorf("Couldn't set QoS: %v", err)
	}

	// consume (pull) the messages
	msgs, err := ch.Consume(
		qu.Name, // queue
		"",      // consumer
		false,   // auto-ack
		false,   // exclusive
		false,   // no-local
		false,   // no-wait
		nil,     // args
	)
	if err != nil {
		return fmt.Errorf("couldn't consume the queue: %v", err)
	}

	go func() {
		defer ch.Close()

		// range over the msgs channel
		for msg := range msgs {

			body, err := unmarshaller(msg.Body)
			if err != nil {
				fmt.Printf("couldn't unmarshal message: %v", err)
				continue
			}

			// handle the messages
			ackType := handler(body)
			switch ackType {
			case Ack: // acknowledge
				msg.Ack(false)

			case NackRequeue: // negative acknowledge requeue
				msg.Nack(false, true)

			case NackDiscard: // negative acknowledge
				msg.Nack(false, false)
			}
		}
	}()

	return nil
}

// SubscribeJSON connect to the queue and pull the messages
func SubscribeJSON[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType, // an enum to represent "durable" or "transient"
	handler func(T) AckType, // an enum to represent "durable" or "transient"
) error {
	return subscribe[T](
		conn,
		exchange,
		queueName,
		key,
		queueType,
		handler,
		func(data []byte) (T, error) {
			var body T
			err := json.Unmarshal(data, &body)
			return body, err
		},
	)
}

func SubscribeGob[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType, // an enum to represent "durable" or "transient"
	handler func(T) AckType, // an enum to represent "durable" or "transient"
) error {
	return subscribe[T](
		conn,
		exchange,
		queueName,
		key,
		queueType,
		handler,
		func(data []byte) (T, error) {
			buf := bytes.NewBuffer(data)
			decoder := gob.NewDecoder(buf)
			var body T
			err := decoder.Decode(&body)
			return body, err
		},
	)
}
