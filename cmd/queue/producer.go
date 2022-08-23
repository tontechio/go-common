package main

import (
	"log"

	"github.com/plimble/zap-sentry"
	"github.com/spf13/cobra"
	"github.com/tontechio/go-common/pkg/amqp"
	"github.com/tontechio/go-common/pkg/queue"
)

var producer = &cobra.Command{
	Use:   "producer",
	Short: "Deferred queue producer",
	Run:   runProducer,
}

func init() {
	// set flags
	setCfg(producer.Flags())

	// add command
	rootCmd.AddCommand(producer)
}

func runProducer(cmd *cobra.Command, args []string) {
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
		cfg.queue.name,
		logger,
	)
	if err != nil {
		logger.Fatalf("queue.NewProducer error: %v", err)
	}

	err = producer.Connect()
	if err != nil {
		logger.Fatalf("producer.Connect error: %v", err)
	}

	for i := 0; i < cfg.queue.limit*3; i++ {
		err = producer.EmitMessage("test message")
		if err != nil {
			logger.Fatalf("producer.EmitMessage error: %v", err)
		}
	}
}
