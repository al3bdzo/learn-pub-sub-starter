package main

import (
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"

	"os"
	"os/signal"
	"log"
	"fmt"
)

func main() {
	fmt.Println("Starting Peril client...")

	connectionString := "amqp://guest:guest@localhost:5672/"
	conn, err := amqp.Dial(connectionString)
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	fmt.Println("Connection to RabbitMQ was successful!!")

	username, err := gamelogic.ClientWelcome()
	if err != nil {
		log.Fatal(err)
	}

	_, queue, err := pubsub.DeclareAndBind(
		conn, 
		routing.ExchangePerilDirect, 
		fmt.Sprintf("%s.%s", routing.PauseKey, username),
		routing.PauseKey,
		pubsub.SimpleQueueTransient,
	)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Queue %v declared and bound!\n", queue.Name)

	gameState := gamelogic.NewGameState(username)
	for {
		userInput := gamelogic.GetInput()
		if len(userInput) == 0 {
			continue
		}

		switch userInput[0]{
		case "spawn":
			err = gameState.CommandSpawn(userInput)
			if err != nil {
				fmt.Println(err)
				continue
			}
		case "move":
			_, err = gameState.CommandMove(userInput)
			if err != nil {
				fmt.Println(err)
				continue
			}
		case "status":
			gameState.CommandStatus()
		case "help":
			gamelogic.PrintClientHelp()
		case "spam":
			fmt.Println("Spamming is not allowed yet!")
		case "quit":
			gamelogic.PrintQuit()
			return 
		default:
			fmt.Println("No such command!")
		}
	}

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)
	if _, ok := <-signalChan; ok {
		fmt.Println("Client is shutting down!")
	}
}
