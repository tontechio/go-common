package graceful

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"golang.org/x/sync/errgroup"
)

// Logger interface
type logger interface {
	Error(args ...interface{})
}

// Listen graceful
func Listen(ctx context.Context, run, stop func() error, logger logger) error {
	// define context
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// run
	g, ctx := errgroup.WithContext(ctx)
	g.Go(run)

	// init interrupt chan
	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)

	select {
	case <-interrupt:
		break
	case <-ctx.Done():
		break
	}

	cancel()

	// stop
	if err := stop(); err != nil {
		logger.Error("close error", err)
	}

	if err := g.Wait(); err != nil {
		return fmt.Errorf("shutdown failed with error: %w", err)
	}

	return nil
}
