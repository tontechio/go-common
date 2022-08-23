package main

import (
	"log"

	"github.com/plimble/zap-sentry"
	"github.com/spf13/cobra"
	"github.com/tontechio/go-common/pkg/amqp"
	"github.com/tontechio/go-common/pkg/queue"
)

var consumer = &cobra.Command{
	Use:   "consumer",
	Short: "Deferred queue consumer",
	Run:   runConsumer,
}

func init() {
	// set flags
	setCfg(consumer.Flags())

	// add command
	rootCmd.AddCommand(consumer)
}

func runConsumer(cmd *cobra.Command, args []string) {
	// cfg
	cfg, err := getCfg(cmd.Flags())
	if err != nil {
		log.Fatalf("getCfg error: %v", err)
	}

	// logger
	loggerOpts := zapsentry.WithSentry(cfg.logger.host, nil, nil)
	logger := zapsentry.New(loggerOpts)
	defer logger.Sync()

	connectionProvider, err := amqp.NewConnectionProvider(
		cfg.amqp.host,
		cfg.amqp.port,
		cfg.amqp.username,
		cfg.amqp.password,
		cfg.amqp.vhost,
	)
	if err != nil {
		logger.Fatalf("amqp.NewConnectionProvider error: %v", err)
	}

	// producer
	producer, err := queue.NewProducer(
		connectionProvider,
		cfg.queue.exchange,
		cfg.queue.routingKey,
		logger,
	)
	if err != nil {
		logger.Fatalf("queue.NewProducer error: %v", err)
	}

	err = producer.Connect()
	if err != nil {
		logger.Fatalf("producer.Connect error: %v", err)
	}

	// listener
	handler, err := newHandler(producer)
	if err != nil {
		logger.Fatalf("newHandler error: %v", err)
	}

	listener, err := queue.NewListener(
		connectionProvider,
		cfg.queue.exchange,
		queue.DeliveryToOneOfAllListeners,
		cfg.queue.name,
		cfg.queue.routingKey,
		queue.OnErrorRequeue,
		logger,
	)
	if err != nil {
		logger.Fatalf("queue.NewListener error: %v", err)
	}

	err = listener.Process(handler, cfg.queue.limit)
	if err != nil {
		logger.Fatalf("listener.Process error: %v", err)
	}
}
