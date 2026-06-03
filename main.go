package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func main() {
	// Setup OS signal channel for graceful shutdown
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	// Configure the Kafka Consumer with optimized rebalance settings
	config := &kafka.ConfigMap{
		"bootstrap.servers":             "localhost:9092",
		"group.id":                      "optimized-consumer-group",
		"auto.offset.reset":             "earliest",
		"enable.auto.commit":            false, // Commit manually to control rebalance behavior
		"partition.assignment.strategy": "cooperative-sticky",
		"session.timeout.ms":            45000,  // 45 seconds to handle transient network hiccups
		"heartbeat.interval.ms":         3000,   // 3 seconds to maintain active group membership
		"max.poll.interval.ms":          300000, // 5 minutes to accommodate slow processing of batches
	}

	consumer, err := kafka.NewConsumer(config)
	if err != nil {
		log.Fatalf("Failed to create consumer: %s", err)
	}
	defer consumer.Close()

	// Subscribe to the topic with a custom, non-blocking rebalance callback
	err = consumer.SubscribeTopics([]string{"test-rebalance-topic"}, func(c *kafka.Consumer, event kafka.Event) error {
		switch ev := event.(type) {
		case kafka.AssignedPartitions:
			log.Printf("Partitions assigned: %v", ev.Partitions)
			// Assign partitions incrementally without blocking the main thread
			err := c.Assign(ev.Partitions)
			if err != nil {
				log.Printf("Error assigning partitions: %s", err)
			}
		case kafka.RevokedPartitions:
			log.Printf("Partitions revoked: %v", ev.Partitions)
			// Commit offsets asynchronously to prevent blocking the rebalance loop
			go func() {
				_, commitErr := c.Commit()
				if commitErr != nil {
					log.Printf("Asynchronous commit failed during revocation: %s", commitErr)
				} else {
					log.Println("Asynchronous commit succeeded during revocation")
				}
			}()
			// Unassign partitions incrementally
			err := c.Unassign()
			if err != nil {
				log.Printf("Error unassigning partitions: %s", err)
			}
		}
		return nil
	})

	if err != nil {
		log.Fatalf("Failed to subscribe to topics: %s", err)
	}

	log.Println("Consumer started successfully. Listening for messages...")

	// Main poll loop
	run := true
	for run {
		select {
		case sig := <-sigchan:
			log.Printf("Caught signal %v: terminating", sig)
			run = false
		default:
			// Poll for events/messages
			ev := consumer.Poll(100)
			if ev == nil {
				continue
			}

			switch e := ev.(type) {
			case *kafka.Message:
				log.Printf("Processed message on %s [%d] at offset %v: %s",
					*e.TopicPartition.Topic, e.TopicPartition.Partition, e.TopicPartition.Offset, string(e.Value))
				// Commit offsets asynchronously after processing
				_, err := consumer.CommitMessage(e)
				if err != nil {
					log.Printf("Failed to commit message offset: %s", err)
				}
			case kafka.Error:
				log.Printf("Kafka error: %v", e)
				if e.IsFatal() {
					run = false
				}
			default:
				// Ignore other event types
			}
		}
	}
}
