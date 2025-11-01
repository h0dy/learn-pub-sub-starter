package main

import (
	"fmt"
	"log"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

// handlerPause pauses the game for the client
func handlerPause(gs *gamelogic.GameState) func(routing.PlayingState) pubsub.AckType {
	return func(r routing.PlayingState) pubsub.AckType {
		defer fmt.Print("> ")
		gs.HandlePause(r)
		return pubsub.Ack
	}
}

// handlerMove moves the army for the client
func handlerMove(gs *gamelogic.GameState, ch *amqp.Channel) func(gamelogic.ArmyMove) pubsub.AckType {
	return func(army gamelogic.ArmyMove) pubsub.AckType {
		defer fmt.Print("> ")
		mv := gs.HandleMove(army)
		switch mv {
		case gamelogic.MoveOutComeSafe:
			return pubsub.Ack

		case gamelogic.MoveOutcomeMakeWar:
			if err := pubsub.PublishJSON(
				ch,
				routing.ExchangePerilTopic,
				routing.WarRecognitionsPrefix+"."+gs.GetUsername(),
				gamelogic.RecognitionOfWar{
					Attacker: army.Player,
					Defender: gs.GetPlayerSnap(),
				}); err != nil {
				log.Fatalf("Couldn't send acknowledge : %v\n", err)
				return pubsub.NackRequeue
			}
			return pubsub.Ack

		case gamelogic.MoveOutcomeSamePlayer:
			return pubsub.NackDiscard
		}

		fmt.Println("Error: Unknown move")
		return pubsub.NackDiscard
	}
}

// handlerWar declares a war for the client
func handlerWar(gs *gamelogic.GameState, ch *amqp.Channel) func(dw gamelogic.RecognitionOfWar) pubsub.AckType {
	return func(dw gamelogic.RecognitionOfWar) pubsub.AckType {
		defer fmt.Print("> ")
		warOutcome, winner, loser := gs.HandleWar(dw)
		switch warOutcome {
		case gamelogic.WarOutcomeNotInvolved:
			return pubsub.NackRequeue

		case gamelogic.WarOutcomeNoUnits:
			return pubsub.NackDiscard

		case gamelogic.WarOutcomeYouWon:
			if err := publishGameLog(
				ch,
				gs.GetUsername(),
				fmt.Sprintf("%v won a war against %v\n", winner, loser),
			); err != nil {
				fmt.Printf("error in war outcome: %v\n", err)
				return pubsub.NackRequeue
			}
			return pubsub.Ack

		case gamelogic.WarOutcomeOpponentWon:
			if err := publishGameLog(
				ch,
				gs.GetUsername(),
				fmt.Sprintf("%v won a war against %v\n", winner, loser),
			); err != nil {
				fmt.Printf("error in war outcome: %v\n", err)
				return pubsub.NackRequeue
			}
			return pubsub.Ack

		case gamelogic.WarOutcomeDraw:
			if err := publishGameLog(
				ch,
				gs.GetUsername(),
				fmt.Sprintf("A war between %v and %v resulted in a draw", winner, loser),
			); err != nil {
				fmt.Printf("error in war outcome draw: %v\n", err)
				return pubsub.NackRequeue
			}
			return pubsub.Ack
		}

		fmt.Println("Error: Unknown war outcome")
		return pubsub.NackDiscard
	}
}
