package queue

import (
	"fmt"
	"time"

	"github.com/streadway/amqp"
)

// DeliveryType ...
type DeliveryType string

// Delivery types
const (
	DeliveryToAllListeners      DeliveryType = "fanout"
	DeliveryToOneOfAllListeners DeliveryType = "topic"
)

// OnErrorBehaviour ...
type OnErrorBehaviour int8

// On error ...
const (
	OnErrorAck OnErrorBehaviour = iota
	OnErrorNack
	OnErrorReject
	OnErrorRequeue
	OnErrorDefer
	OnErrorNackSleep
)

// Max message processing attempts before die
const maxAttempts = 100

// MessageHandler ...
type MessageHandler interface {
	HandleMessage(msg []byte) error
}

// Logger
type Logger interface {
	Debug(args ...interface{})
	Error(args ...interface{})
}

// Publish message to queue
func emit(ch *amqp.Channel, exchangeName, queueName string, msg []byte, timestamp time.Time, headers headers) error {
	publishing := amqp.Publishing{
		ContentType: "text/plain",
		Body:        msg,
		Timestamp:   timestamp,
		Headers:     headers,
	}
	return ch.Publish(
		exchangeName, // exchange
		queueName,    // routing key
		false,        // mandatory
		false,        // immediate
		publishing)
}

// Returns queue name for deferred messages
func getDeferredQueueName(queueName string) string {
	return fmt.Sprintf("%s_error", queueName)
}
