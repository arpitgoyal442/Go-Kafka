package main

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"sync"
	"time"
	"github.com/Shopify/sarama"
)


func producer(config *sarama.Config, wg *sync.WaitGroup, done chan bool) {

	<-done

	defer wg.Done()

	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 5
	config.Producer.Return.Successes = true

	// Create a new Kafka producer
	producer, err := sarama.NewSyncProducer([]string{"localhost:9092"}, config)
	if err != nil {
		log.Fatalf("Failed to create producer: %s", err)
	}
	defer producer.Close()

	topic := "mytesttopic"

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	// Counter for messages
	counter := 0
	for {
		select{

		case <-signals:
			fmt.Println("Shutting down Producer...")
			return

		default :

		// Introducing random delay
		delay := time.Duration(rand.Intn(5)) * time.Second
		time.Sleep(delay)

		// Get current UTC time
		currentTime := time.Now().UTC()

		// Prepare message value
		messageValue := fmt.Sprintf("(%s, %d)", currentTime.Format(time.RFC3339), counter)

		// Create Kafka message
		message := &sarama.ProducerMessage{
			Topic: topic,
			Value: sarama.StringEncoder(messageValue),
		}

		// Send message
		partition, offset, err := producer.SendMessage(message)
		if err != nil {
			log.Fatalf("Failed to send message: %s", err)
		}

		fmt.Printf("Message sent to partition %d at offset %d: %s\n", partition, offset, messageValue)

		// Incrementing the counter for next message
		counter++

		// Sleep for some time before sending the next message
		time.Sleep(time.Second*1)
		
		}
		
	}
}


func consumer(config *sarama.Config, wg *sync.WaitGroup, done chan bool) {

	defer wg.Done()

	config.Consumer.Return.Errors = true

	// Create a new Kafka consumer
	consumer, err := sarama.NewConsumer([]string{"localhost:9092"}, config)
	if err != nil {
		log.Fatalf("Failed to create consumer: %s", err)
	}
	defer consumer.Close()

	topic := "mytesttopic"

	fmt.Println("Inside Consumer")

	// Set up a partition consumer for the topic
	partitionConsumer, err := consumer.ConsumePartition(topic, 0, sarama.OffsetNewest)
	if err != nil {
		log.Fatalf("Failed to create partition consumer: %s", err)
	}

	done<-true
	defer partitionConsumer.Close()

	// Handle Ctrl+C signal to gracefully close the consumer
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	for {
		select {
		case msg := <-partitionConsumer.Messages():
			messageValue := string(msg.Value)
			fmt.Printf("Received message: partition=%d offset=%d value=%s\n",
				msg.Partition, msg.Offset, messageValue)

		case err := <-partitionConsumer.Errors():
			fmt.Printf("Error: %s\n", err.Error())

		case <-signals:
			fmt.Println("Shutting down consumer...")
			return
		}
	}
}



func main() {
	// Set up configuration for the Kafka producer
	config := sarama.NewConfig()

	done:=make(chan bool,1)

	var wg sync.WaitGroup

	wg.Add(2)

	
	go producer(config,&wg,done)
	go consumer(config,&wg,done)
	
	
	wg.Wait()
	
}
