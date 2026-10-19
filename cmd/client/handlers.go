package main 

import (
	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"


	"fmt"
)



func handlerPause(gs *gamelogic.GameState) func(routing.PlayingState) pubsub.Acktype {
	return func(message routing.PlayingState) pubsub.Acktype {
		defer fmt.Print("> ")
		gs.HandlePause(message)
		return pubsub.Ack
	}
}

func handlerMove(gs *gamelogic.GameState) func(gamelogic.ArmyMove) pubsub.Acktype {
	return func(message gamelogic.ArmyMove) pubsub.Acktype {
		defer fmt.Print("> ")
		moveOutCome := gs.HandleMove(message)
		if moveOutCome == gamelogic.MoveOutComeSafe || moveOutCome == gamelogic.MoveOutcomeMakeWar {
			return pubsub.Ack
		}
		return pubsub.NackDiscard
	}
}