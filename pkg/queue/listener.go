package queue

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/streadway/amqp"
	serviceAmqp "github.com/tontechio/go-common/pkg/amqp"
)

// Listener ...
type Listener struct {
	sync.Mutex
	concurrency            int
	exchangeName           string
	queueName              string
	routingKey             string
	handler                MessageHandler
	maxRetriesCount        int
	amqpChannel            *amqp.Channel
	amqpConnectionProvider *serviceAmqp.ConnectionProvider
	queueType              DeliveryType
	onError                OnErrorBehaviour
	logger                 Logger
	wg                     *sync.WaitGroup
	stopChan               chan bool
}

func (s *Listener) msgProcessor(ch *amqp.Channel, channel chan int, message amqp.Delivery, handler MessageHandler) {
	defer s.wg.Done()
	defer func() {
		<-channel
	}()

	var err error

	message.Headers = incrementHeadersAttempts(message.Headers)
	a := getHeadersAttempts(message.Headers)
	if a <= maxAttempts {
		err = handler.HandleMessage(message.Body)
	} else {
		s.logger.Error(fmt.Errorf("Listener.msgProcessor() message exceedes max attempts, queue = %s, msg = %s, headers = %v", s.queueName, string(message.Body), message.Headers))
	}

	if err != nil {
		s.logger.Error(fmt.Errorf("Listener.msgProcessor() handle message error, queue = %s, msg = %s: %w", s.queueName, string(message.Body), err))

		switch s.onError {
		case OnErrorAck:
			err = message.Ack(false)
		case OnErrorNack:
			err = message.Nack(false, false)
		case OnErrorReject:
			err = message.Reject(false)
		case OnErrorRequeue:
			if err = emit(ch, s.exchangeName, s.queueName, message.Body, message.Timestamp, message.Headers); err != nil {
				s.logger.Error(fmt.Errorf("Listener.msgProcessor() fail to requeue message, queue = %s: %w", s.queueName, err))
				return
			}
			err = message.Ack(false)
			time.Sleep(5 * time.Second)
		case OnErrorDefer:
			queueName := getDeferredQueueName(s.queueName)
			if err = emit(ch, s.exchangeName, queueName, message.Body, message.Timestamp, message.Headers); err != nil {
				s.logger.Error(fmt.Errorf("Listener.msgProcessor() fail to defer message, queue = %s: %w", s.queueName, err))
				return
			}
			err = message.Ack(false)
			time.Sleep(5 * time.Second)
		case OnErrorNackSleep:
			err = message.Nack(false, false)
			time.Sleep(5 * time.Second)
		}
		if err != nil {
			s.logger.Error(fmt.Errorf("Listener.msgProcessor() handle message behaviour error, queue = %s: %w", s.queueName, err))
		}

		return
	}

	// ack on success
	if err := message.Ack(false); err != nil {
		s.logger.Error(fmt.Errorf("Listener.msgProcessor() message.Ack error, queue = %s, msg = %s: %w", s.queueName, string(message.Body), err))
	}
}

func (s *Listener) listen(ch *amqp.Channel, msgs <-chan amqp.Delivery, handler MessageHandler) {
	defer s.wg.Done()

	s.logger.Debug(fmt.Sprintf("Listener.listen() started, queue = %s", s.queueName))

	var concurrencyChan = make(chan int, s.concurrency)

	for {
		select {
		case <-s.stopChan:
			s.logger.Debug(fmt.Sprintf("Listener.listen() processing stopped, queue = %s", s.queueName))
			return
		case msg := <-msgs:
			s.logger.Debug(fmt.Sprintf("Listener.listen() received message, queue = %s, msg = %s, headers = %v", s.queueName, string(msg.Body), msg.Headers))
			concurrencyChan <- 1
			s.wg.Add(1)
			go s.msgProcessor(ch, concurrencyChan, msg, handler)
		}
	}
}

// Listen ...
func (s *Listener) Listen(handler MessageHandler) error {
	s.handler = handler

	connProvider, err := s.amqpConnectionProvider.GetConnection()
	if err != nil {
		return fmt.Errorf("Listener.Listen() unable get connection, exchange = %v, queue = %v: %w", s.exchangeName, s.queueName, err)
	}

	err = s.initListener(connProvider)
	if err != nil {
		return fmt.Errorf("Listener.Listen() unable init channel, exchange = %v, queue = %v: %w", s.exchangeName, s.queueName, err)
	}

	return nil
}

// Process ...
func (s *Listener) Process(handler MessageHandler, limit int) error {
	conn, err := s.amqpConnectionProvider.GetConnection()
	if err != nil {
		return fmt.Errorf("Listener.Process() unable get connection, exchange = %v, queue = %v: %w", s.exchangeName, s.queueName, err)
	}

	ch, err := s.initChannel(conn)
	if err != nil {
		return fmt.Errorf("Listener.Process() unable to init channel: %w", err)
	}

	s.logger.Debug(fmt.Sprintf("Listener.Process() started, queue = %s, limit = %d", s.queueName, limit))

	var concurrencyChan = make(chan int, s.concurrency)

	for i := 0; i < limit; i++ {
		msg, ok, err := ch.Get(s.queueName, false)
		if !ok {
			s.logger.Debug(fmt.Sprintf("Listener.Process() nothing to get, queue = %s, i = %d, limit = %d", s.queueName, i, limit))
			return nil
		}
		if err != nil {
			s.logger.Error(fmt.Sprintf("Listener.Process() get message error, queue = %s", s.queueName))
			return err
		}

		s.logger.Debug(fmt.Sprintf("Listener.Process() received message, queue = %s, msg = %s, headers = %v", s.queueName, string(msg.Body), msg.Headers))
		concurrencyChan <- 1
		s.wg.Add(1)
		go s.msgProcessor(ch, concurrencyChan, msg, handler)
	}

	s.wg.Wait()

	s.logger.Debug(fmt.Sprintf("Listener.Process() processing stopped, queue = %s", s.queueName))

	return nil
}

func (s *Listener) initChannel(conn *amqp.Connection) (*amqp.Channel, error) {
	ch, err := conn.Channel()

	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("Listener.initListener() unable init channel, exchange = %v, queue = %v: %w", s.exchangeName, s.queueName, err)
	}

	err = ch.ExchangeDeclare(
		s.exchangeName,      // name
		string(s.queueType), // type
		true,                // durable
		false,               // auto-deleted
		false,               // internal
		false,               // no-wait
		nil,                 // arguments
	)
	if err != nil {
		ch.Close()
		return nil, fmt.Errorf("Listener.initListener() unable to declare exchange, exchange = %v, queuee = %v: %w", s.exchangeName, s.queueName, err)
	}

	durable := s.queueType == DeliveryToOneOfAllListeners
	exclusive := s.queueType == DeliveryToAllListeners

	q, err := ch.QueueDeclare(
		s.queueName, // name
		durable,     // durable only if topic
		false,       // delete when unused
		exclusive,   // exclusive only for fanout
		false,       // no-wait
		nil,         // arguments
	)
	if err != nil {
		ch.Close()
		return nil, fmt.Errorf("Listener.initListener() unable to declare queue, exchange = %v, queue = %v: %w", s.exchangeName, s.queueName, err)
	}

	err = ch.QueueBind(
		q.Name,         // queue name
		s.routingKey,   // routing key
		s.exchangeName, // exchange
		false,
		nil)
	if err != nil {
		ch.Close()
		return nil, fmt.Errorf("Listener.initListener() unable to bind queue, exchange = %v, queue = %v: %w", s.exchangeName, q.Name, err)
	}

	if err := ch.Qos(1, 0, false); err != nil {
		ch.Close()
		return nil, fmt.Errorf("Listener.initListener() ch.Qos error: %w", err)
	}

	return ch, nil
}

func (s *Listener) initListener(conn *amqp.Connection) error {
	ch, err := s.initChannel(conn)
	if err != nil {
		return fmt.Errorf("Listener.initListener() init channel error: %w", err)
	}

	msgs, err := ch.Consume(
		s.queueName, // queue
		"",          // consumer
		false,       // auto-ack
		false,       // exclusive
		false,       // no-local
		false,       // no-wait
		nil,         // args
	)
	if err != nil {
		ch.Close()
		return fmt.Errorf("Listener.initListener() failed to register a consumer, exchange = %v, queue = %v: %w", s.exchangeName, s.queueName, err)
	}

	s.Lock()
	s.amqpChannel = ch
	s.Unlock()

	// register close listener
	notifyCloseChannel := make(chan *amqp.Error)
	s.amqpChannel.NotifyClose(notifyCloseChannel)

	s.wg.Add(2)
	go s.listen(s.amqpChannel, msgs, s.handler)
	go func() {
		defer s.wg.Done()

		notifyCloseErr := <-notifyCloseChannel

		if notifyCloseErr == nil {
			if err := conn.Close(); err != nil {
				s.logger.Error(fmt.Errorf("Listener.initListener() conn.Close error: %w", err))
			}
			return
		}

		s.logger.Debug(fmt.Errorf("Listener.initListener() amqp channel was closed, code = %v, server = %v, reason = %v, recover = %v, exchange = %v, queue = %v",
			notifyCloseErr.Code, notifyCloseErr.Server, notifyCloseErr.Reason, notifyCloseErr.Recover, s.exchangeName, s.queueName))

		err = s.amqpConnectionProvider.ProvideConnectionToCb(s.initListener, "broadcastQueueListener", s.maxRetriesCount)
		if err != nil {
			s.logger.Error(fmt.Errorf("Listener.initListener() amqpConnectionProvider.ProvideConnectionToCb: %w", err))
		}

	}()

	return nil
}

// Close ...
func (s *Listener) Close() error {
	s.stopChan <- true

	if s.amqpChannel != nil {
		if err := s.amqpChannel.Close(); err != nil {
			return fmt.Errorf("Listener.Close() unable to close channel, exchange = %v: %w", s.exchangeName, err)
		}
	}

	s.wg.Wait()
	return nil
}

// NewListener ...
func NewListener(
	amqpConnectionProvider *serviceAmqp.ConnectionProvider,
	exchangeName string,
	queueType DeliveryType,
	queueName string,
	routingKey string,
	onError OnErrorBehaviour,
	logger Logger,
) (*Listener, error) {
	if amqpConnectionProvider == nil {
		return nil, errors.New("NewListener() connection provider can not be empty")
	}

	if queueType == DeliveryToAllListeners && queueName != "" {
		return nil, errors.New("NewListener() when queue type is DeliveryToAllListeners queue must be empty")
	}

	if queueType == DeliveryToOneOfAllListeners && queueName == "" {
		return nil, errors.New("NewListener() when queue type is DeliveryToOneOfAllListeners queue should have name")
	}

	return &Listener{
		amqpConnectionProvider: amqpConnectionProvider,
		exchangeName:           exchangeName,
		concurrency:            1,
		maxRetriesCount:        64, // TODO: get from user
		queueType:              queueType,
		queueName:              queueName,
		routingKey:             routingKey,
		onError:                onError,
		logger:                 logger,
		wg:                     &sync.WaitGroup{},
		stopChan:               make(chan bool, 1),
	}, nil

}
