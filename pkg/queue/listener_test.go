package queue

import (
	"testing"

	serviceAmqp "github.com/tontechio/go-common/pkg/amqp"
)

func getTestConnectionProvider(t *testing.T) *serviceAmqp.ConnectionProvider {
	cp, err := serviceAmqp.NewConnectionProvider("rabbitmq", 5672, "rabbitmq", "rabbitmq", "")
	if err != nil {
		t.Fatal("serviceAmqp.NewConnectionProvider")
	}
	return cp
}

type nopLogger struct{}

func (l *nopLogger) Debug(args ...interface{}) {}
func (l *nopLogger) Error(args ...interface{}) {}

func TestNewListener(t *testing.T) {
	type args struct {
		amqpConnectionProvider *serviceAmqp.ConnectionProvider
		exchangeName           string
		queueType              DeliveryType
		queueName              string
		routingKey             string
		onError                OnErrorBehaviour
		logger                 Logger
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "init",
			args: args{
				amqpConnectionProvider: getTestConnectionProvider(t),
				exchangeName:           "",
				queueType:              "",
				queueName:              "",
				routingKey:             "",
				onError:                0,
				logger:                 nil,
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NewListener(tt.args.amqpConnectionProvider, tt.args.exchangeName, tt.args.queueType, tt.args.queueName, tt.args.routingKey, tt.args.onError, tt.args.logger)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewListener() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			t.Logf("NewListener() got = %v", got)
		})
	}
}
