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