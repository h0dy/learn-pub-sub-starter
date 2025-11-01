package main

import (
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	// connect to the broker
	rabbitUri := "amqp://guest:guest@localhost:5672/"
	rabbitConn, err := amqp.Dial(rabbitUri)
	if err != nil {
		log.Fatal("Error in connecting to rabbitMQ")
	}
	defer rabbitConn.Close()
	fmt.Println("Peril game client connected to RabbitMQ!")

	// create channel to create queues, exchanges, and publish messages
	ch, err := rabbitConn.Channel()
	if err != nil {
		log.Fatalf("couldn't create a channel: %v", err)
	}

	username, err := gamelogic.ClientWelcome()
	if err != nil {
		log.Fatalf("Couldn't get the username: %v\n", err)
	}

	gameState := gamelogic.NewGameState(username)

	// subscribe for (consume) pausing queues
	if err = pubsub.SubscribeJSON(
		rabbitConn,                    // broker connection
		routing.ExchangePerilDirect,   // exchange type
		routing.PauseKey+"."+username, // queue name
		routing.PauseKey,              // routing key
		pubsub.Transient,              // queue type
		handlerPause(gameState),       // handler (handle pausing)
	); err != nil {
		log.Fatal(err)
	}

	// subscribe for (consume) move player queues
	if err = pubsub.SubscribeJSON(
		rabbitConn,                           // broker connection
		routing.ExchangePerilTopic,           // exchange type
		routing.ArmyMovesPrefix+"."+username, // queue name
		routing.ArmyMovesPrefix+".*",         // routing key (wildcard)
		pubsub.Transient,                     // queue type
		handlerMove(gameState, ch),           // handler (handle move)
	); err != nil {
		log.Fatal(err)
	}

	// subscribe for (consume) war declaration queues
	if err = pubsub.SubscribeJSON(
		rabbitConn,
		routing.ExchangePerilTopic,
		routing.WarRecognitionsPrefix,
		routing.WarRecognitionsPrefix+".*",
		pubsub.Durable,
		handlerWar(gameState, ch),
	); err != nil {
		log.Fatalf("could not subscribe to war declarations: %v", err)
	}

	// start REPL
	for {
		words := gamelogic.GetInput()
		input := words[0]

		if len(words) == 0 {
			continue
		}
		switch input {
		case "spawn":
			if err := gameState.CommandSpawn(words); err != nil {
				fmt.Println(err)
				continue
			}

		case "move":
			mv, err := gameState.CommandMove(words)
			if err != nil {
				fmt.Println(err)
				continue
			}

			// publish (exchange) a move message
			if err = pubsub.PublishJSON(
				ch,                                   // channel
				routing.ExchangePerilTopic,           // exchange type
				routing.ArmyMovesPrefix+"."+username, // routing key
				mv,
			); err != nil {
				fmt.Println(err)
				continue
			}
			fmt.Println("published successfully")

		case "status":
			gameState.CommandStatus()

		case "help":
			gamelogic.PrintClientHelp()

		case "spam":
			if len(words) < 2 {
				fmt.Println("Please provide the spam amount")
				return
			}
			spams, err := strconv.Atoi(words[1])
			if err != nil {
				fmt.Printf("error: %v is not a valid number\n", words[1])
				continue
			}

			for range spams {
				msg := gamelogic.GetMaliciousLog()
				if err := publishGameLog(ch, username, msg); err != nil {
					fmt.Printf("error publishing malicious log: %v\n", err)
				}
			}
			fmt.Printf("Published %v malicious logs\n", spams)

		case "quit":
			gamelogic.PrintQuit()
			return

		default:
			fmt.Println("couldn't understand the command")
			continue
		}
	}
}

func publishGameLog(ch *amqp.Channel, username, msg string) error {
	return pubsub.PublishGob(
		ch,
		routing.ExchangePerilTopic,
		routing.GameLogSlug+"."+username,
		routing.GameLog{
			Username:    username,
			CurrentTime: time.Now(),
			Message:     msg,
		},
	)
}
