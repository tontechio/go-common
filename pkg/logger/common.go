package logger

import (
	"errors"
	"fmt"

	zapsentry "github.com/plimble/zap-sentry"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

const (
	outputPath = "/dev/stdout"
	errorPath  = "/dev/stderr"
)

// ConfigBuilder should provide config for a new logger.
type ConfigBuilder interface {
	Build() (*zap.Config, error)
}

// DefaultConfigBuilder is default implementation of interface ConfigBuilder.
type DefaultConfigBuilder struct {
	Level string
}

// Build creates configuration for a logger.
func (c *DefaultConfigBuilder) Build() (*zap.Config, error) {
	logsLevel := zapcore.DebugLevel
	if err := logsLevel.Set(c.Level); err != nil {
		return nil, fmt.Errorf("Can't set level %s error: %w", c.Level, err)
	}

	return &zap.Config{
		Level:       zap.NewAtomicLevelAt(logsLevel),
		Development: false,
		Encoding:    "console",
		EncoderConfig: zapcore.EncoderConfig{
			TimeKey:        "d",
			LevelKey:       "l",
			NameKey:        "n",
			CallerKey:      "c",
			MessageKey:     "m",
			StacktraceKey:  "",
			LineEnding:     zapcore.DefaultLineEnding,
			EncodeLevel:    zapcore.LowercaseLevelEncoder,
			EncodeTime:     zapcore.ISO8601TimeEncoder,
			EncodeCaller:   zapcore.ShortCallerEncoder,
			EncodeDuration: zapcore.SecondsDurationEncoder,
		},
		OutputPaths:      []string{outputPath},
		ErrorOutputPaths: []string{errorPath},
	}, nil
}

// NewDummyLogger ...
func NewDummyLogger(level string) (*zap.Logger, error) {
	return NewLogger("", &DefaultConfigBuilder{level})
}

//NewLogger create a new logger
func NewLogger(sentryURL string, loggerConfigBuilder ConfigBuilder) (*zap.Logger, error) {
	if loggerConfigBuilder == nil {
		return nil, errors.New("loggerConfigBuilder is nil")
	}

	config, err := loggerConfigBuilder.Build()
	if err != nil {
		return nil, fmt.Errorf("loggerConfigBuilder.Build() error: %w", err)
	}

	defaultLogger, err := config.Build()
	if err != nil {
		return nil, fmt.Errorf("config.Build() error: %w", err)
	}

	if sentryURL == "" {
		return defaultLogger, nil
	}

	return addSentryCore(defaultLogger, sentryURL)
}

func addSentryCore(defaultLogger *zap.Logger, sentryURL string) (*zap.Logger, error) {
	zapSentry := zapsentry.Configuration{DSN: sentryURL}
	zapSentryCore, err := zapSentry.Build()
	if err != nil {
		return nil, fmt.Errorf("can't build zap Sentry core: %w", err)
	}

	return defaultLogger.WithOptions(zap.WrapCore(func(core zapcore.Core) zapcore.Core {
		return zapcore.NewTee(core, zapSentryCore)
	})), nil
}
