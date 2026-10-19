package main 

import (
	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"

	"time"
	"fmt"
)



func handlerPause(gs *gamelogic.GameState) func(routing.PlayingState) pubsub.Acktype {
	return func(message routing.PlayingState) pubsub.Acktype {
		defer fmt.Print("> ")
		gs.HandlePause(message)
		return pubsub.Ack
	}
}

func handlerMove(gs *gamelogic.GameState, ch *amqp.Channel) func(gamelogic.ArmyMove) pubsub.Acktype {
	return func(message gamelogic.ArmyMove) pubsub.Acktype {
		defer fmt.Print("> ")
		switch(gs.HandleMove(message)) {
		case gamelogic.MoveOutcomeSamePlayer:
			return pubsub.Ack
		case gamelogic.MoveOutComeSafe:
			return pubsub.Ack
		case gamelogic.MoveOutcomeMakeWar:
			err := pubsub.PublishJSON(
				ch,
				routing.ExchangePerilTopic,
				fmt.Sprintf("%s.%s", routing.WarRecognitionsPrefix, gs.GetUsername()),
				gamelogic.RecognitionOfWar{
					Attacker: message.Player,
					Defender: gs.GetPlayerSnap(),
				},
			)
			if err != nil {
				fmt.Printf("error: %s\n", err)
				return pubsub.NackRequeue
			}
			return pubsub.Ack
		}
		return pubsub.NackDiscard
	}
}

func handlerWar(gs *gamelogic.GameState, ch *amqp.Channel) func(gamelogic.RecognitionOfWar) pubsub.Acktype {
	return func(message gamelogic.RecognitionOfWar) pubsub.Acktype {
		defer fmt.Print("> ")
		outCome, winner, loser := gs.HandleWar(message)

		switch(outCome) {
		case gamelogic.WarOutcomeNotInvolved:
			return pubsub.NackRequeue

		case gamelogic.WarOutcomeNoUnits:
			return pubsub.NackDiscard

		case gamelogic.WarOutcomeOpponentWon, gamelogic.WarOutcomeYouWon:
			err := publishGameLog(
				ch, 
				fmt.Sprintf("%s won a war against %s", winner, loser, winner, loser), 
				gs.GetUsername(),
			)
			if err != nil {
				return pubsub.NackRequeue
			}
			return pubsub.Ack

		case gamelogic.WarOutcomeDraw:
			err := publishGameLog(
				ch, 
				fmt.Sprintf("A war between %s and %s resulted in a draw", winner, loser), 
				gs.GetUsername(),
			)
			if err != nil {
				return pubsub.NackRequeue
			}
			return pubsub.Ack

		}

		fmt.Println("What the hell happened in this war!")
		return pubsub.NackDiscard
	}
}


func publishGameLog(ch *amqp.Channel, message, username string) error {
	err := pubsub.PublishGOB(
		ch,
		routing.ExchangePerilTopic,
		fmt.Sprintf("%s.%s", routing.GameLogSlug, username),
		routing.GameLog{
			CurrentTime: time.Now(),
			Message: message,
			Username: username,
		},
	)
	if err != nil {
		return err
	}
	return nil
}