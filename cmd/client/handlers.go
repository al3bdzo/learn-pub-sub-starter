package main 

import (
	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"

	"fmt"
)



func handlerPause(gs *gamelogic.GameState) func(routing.PlayingState) {
	return func(message routing.PlayingState) {
		defer fmt.Print("> ")
		gs.HandlePause(message)
	}
}

func handlerMove(gs *gamelogic.GameState) func(gamelogic.ArmyMove) {
	return func(message gamelogic.ArmyMove) {
		defer fmt.Print("> ")
		gs.HandleMove(message)
	}
}