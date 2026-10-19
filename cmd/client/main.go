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

	gameState := gamelogic.NewGameState(username)
	err = pubsub.SubscribeJSON(
		conn, 
		routing.ExchangePerilDirect, 
		fmt.Sprintf("%s.%s", routing.PauseKey, username),
		routing.PauseKey,
		pubsub.SimpleQueueTransient,
		handlerPause(gameState),
	)
	if err != nil {
		log.Fatal(err)
	}

	ch, err := conn.Channel()
	if err != nil {
		log.Fatal(err)
	}

	err = pubsub.SubscribeJSON(
		conn,
		routing.ExchangePerilTopic,
		fmt.Sprintf("%s.%s", routing.ArmyMovesPrefix, username),
		fmt.Sprintf("%s.*", routing.ArmyMovesPrefix),
		pubsub.SimpleQueueTransient,
		handlerMove(gameState, ch),
	)
	if err != nil {
		log.Fatal(err)
	}

	err = pubsub.SubscribeJSON(
		conn, 
		routing.ExchangePerilTopic, 
		routing.WarRecognitionsPrefix,
		fmt.Sprintf("%s.*", routing.WarRecognitionsPrefix),
		pubsub.SimpleQueueDurable,
		handlerWar(gameState),
	)
	if err != nil {
		log.Fatal(err)
	}

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
			move, err := gameState.CommandMove(userInput)
			if err != nil {
				fmt.Println(err)
				continue
			}
			err = pubsub.PublishJSON(
				ch,
				routing.ExchangePerilTopic,
				fmt.Sprintf("%s.%s", routing.ArmyMovesPrefix, username),
				move,
			)
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


