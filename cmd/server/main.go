package main

import (
	"fmt"
	"log"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	// connect to to the broker
	rabbitUri := "amqp://guest:guest@localhost:5672/"
	fmt.Println("Starting Peril server...")
	rabbitConn, err := amqp.Dial(rabbitUri)
	if err != nil {
		log.Fatal("Error in connecting to rabbitMQ")
	}
	defer rabbitConn.Close()

	// create channel to create queues, exchanges, and publish messages
	ch, err := rabbitConn.Channel()
	if err != nil {
		log.Fatalf("couldn't create a channel: %v", err)
	}

	fmt.Println("successfully connecting to RabbitMQ")

	gamelogic.PrintServerHelp()

	// bind to queue
	if err := pubsub.SubscribeGob(
		rabbitConn,                 // broker connection
		routing.ExchangePerilTopic, // exchange type
		routing.GameLogSlug,        // queue name
		routing.GameLogSlug+".*",   // key (wildcard)
		pubsub.Durable,             // queue type
		handlerLogs(),
	); err != nil {
		log.Fatalf("could not starting consuming logs: %v", err)
	}

	for {
		inputs := gamelogic.GetInput()
		if len(inputs) == 0 {
			continue
		}
		input := inputs[0]
		var isPaused bool

		switch input {
		case "pause":
			fmt.Println("sending a pause message")
			isPaused = true

		case "resume":
			fmt.Println("sending a resume message")
			isPaused = false

		case "quit":
			gamelogic.PrintQuit()
			return

		default:
			fmt.Println("unknown command")
			continue
		}

		// publish (exchange) a pause message
		if err = pubsub.PublishJSON(
			ch,
			routing.ExchangePerilDirect,
			routing.PauseKey,
			routing.PlayingState{
				IsPaused: isPaused,
			}); err != nil {
			log.Fatalf("Couldn't send a pause message: %v\n", err)
		}
	}
}
