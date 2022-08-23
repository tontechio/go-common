package queue

import (
	"testing"

	serviceAmqp "github.com/tontechio/go-common/pkg/amqp"
)

func TestNewProducer(t *testing.T) {
	type args struct {
		amqpConnectionProvider *serviceAmqp.ConnectionProvider
		exchangeName           string
		routingKey             string
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
				routingKey:             "",
				logger:                 &nopLogger{},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NewProducer(tt.args.amqpConnectionProvider, tt.args.exchangeName, tt.args.routingKey, tt.args.logger)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewProducer() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			t.Logf("NewProducer() got = %v", got)
		})
	}
}
