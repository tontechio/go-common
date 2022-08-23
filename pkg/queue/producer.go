package queue

import (
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/streadway/amqp"
	serviceAmqp "github.com/tontechio/go-common/pkg/amqp"
)

// Producer ...
type Producer struct {
	sync.Mutex
	exchangeName           string
	routingKey             string
	maxRetriesCount        int
	amqpChannel            *amqp.Channel
	amqpConnectionProvider *serviceAmqp.ConnectionProvider
	logger                 Logger
	wg                     *sync.WaitGroup
}

// Connect ...
func (s *Producer) Connect() error {
	conn, err := s.amqpConnectionProvider.GetConnection()
	if err != nil {
		return fmt.Errorf("Producer.Connect() unable get connection, exchange = %v: %w", s.exchangeName, err)
	}

	err = s.initProducer(conn)
	if err != nil {
		if closeErr := conn.Close(); closeErr != nil {
			s.logger.Error(fmt.Errorf("Producer.Connect() connection close error: %w", closeErr))
		}

		return fmt.Errorf("Producer.Connect() unable init producer, exchange = %v: %w", s.exchangeName, err)
	}

	return nil
}

func (s *Producer) initProducer(conn *amqp.Connection) error {
	ch, err := conn.Channel()
	if err != nil {
		return fmt.Errorf("Producer.initProducer() unable init channel, exchange = %v: %w", s.exchangeName, err)
	}

	s.Lock()
	s.amqpChannel = ch
	s.Unlock()

	// register close channel
	notifyCloseChannel := make(chan *amqp.Error)
	s.amqpChannel.NotifyClose(notifyCloseChannel)

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()

		// wait until closed
		notifyCloseErr := <-notifyCloseChannel

		if notifyCloseErr == nil {
			if err := conn.Close(); err != nil {
				s.logger.Error(fmt.Errorf("Producer.initProducer() conn.Close error: %w", err))
			}
			return
		}

		s.logger.Debug(fmt.Errorf("Producer.initProducer() amqp channel was closed, code = %v, server = %v, reason = %v, recover = %v, exchange = %v, routing = %v",
			notifyCloseErr.Code, notifyCloseErr.Server, notifyCloseErr.Reason, notifyCloseErr.Recover, s.exchangeName, s.routingKey))

		err = s.amqpConnectionProvider.ProvideConnectionToCb(s.initProducer, "broadcastQueueProducer", s.maxRetriesCount)
		if err != nil {
			s.logger.Error(fmt.Errorf("Producer.initProducer() amqpConnectionProvider.ProvideConnectionToCb error: %w", err))
		}

	}()

	return nil
}

func (s *Producer) EmitMessage(msg interface{}) error {
	encodedData, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("Producer.EmitMessage() can not marshal data, msg = %v: %w", msg, err)
	}

	return s.Emit(encodedData)
}

func (s *Producer) Emit(msg []byte) error {
	err := emit(s.amqpChannel, s.exchangeName, s.routingKey, msg, time.Now(), newHeaders())
	if err != nil {
		return fmt.Errorf("Producer.Emit() unable to publish message, exchange = %v: %w", s.exchangeName, err)
	}

	return nil
}

// Close ...
func (s *Producer) Close() error {
	if s.amqpChannel != nil {
		if err := s.amqpChannel.Close(); err != nil {
			return fmt.Errorf("Producer.Close() unable to close channel, exchange = %v: %w", s.exchangeName, err)
		}
	}

	s.wg.Wait()
	return nil
}

// NewProducer ...
func NewProducer(
	amqpConnectionProvider *serviceAmqp.ConnectionProvider,
	exchangeName string,
	routingKey string,
	logger Logger,
) (*Producer, error) {

	if amqpConnectionProvider == nil {
		return nil, errors.New("queue.NewProducer() connection provider can not be empty")
	}

	if logger == nil {
		return nil, errors.New("queue.NewProducer() logger can not be empty")
	}

	return &Producer{
		amqpConnectionProvider: amqpConnectionProvider,
		exchangeName:           exchangeName,
		routingKey:             routingKey,
		maxRetriesCount:        64, // TODO: get from user
		logger:                 logger,
		wg:                     &sync.WaitGroup{},
	}, nil

}
