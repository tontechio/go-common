package amqp

import (
	"errors"
	"fmt"
	"net"
	"time"

	"github.com/streadway/amqp"
)

// default connection timeout
const connectionTimeout = 10 * time.Second

type ConnectionProvider struct {
	amqpURL string
}

func (p *ConnectionProvider) GetConnection() (*amqp.Connection, error) {
	connection, err := amqp.DialConfig(p.amqpURL, amqp.Config{
		Dial: func(network, addr string) (net.Conn, error) {
			return net.DialTimeout(network, addr, connectionTimeout)
		},
	})
	if err != nil {
		return nil, fmt.Errorf("ConnectionProvider.GetConnection() unable to init amqp connection with url = %s, timeout = %v: %w", p.amqpURL, connectionTimeout, err)
	}

	return connection, nil
}

func (p *ConnectionProvider) ProvideConnectionToCb(cb func(connection *amqp.Connection) error, cbName string, maxRetriesCount int) error {
	if maxRetriesCount == 0 {
		conn, err := p.GetConnection()
		if err != nil {
			return fmt.Errorf("ConnectionProvider.ProvideConnectionToCb() unable to get connection: %w", err)
		}

		return cb(conn)
	}

	var resultError error
	var connection *amqp.Connection

	for i := 1; i <= maxRetriesCount; i++ {
		connection, resultError = p.GetConnection()
		if resultError == nil {
			resultError = cb(connection)
			if resultError == nil {
				break
			}

			if connection != nil {
				connection.Close()
			}
		}

		// sleep before reconnect
		sleepDuration := time.Second * time.Duration(i)
		time.Sleep(sleepDuration)
	}

	if resultError != nil {
		return fmt.Errorf("ConnectionProvider.ProvideConnectionToCb() unable to connect to amqp with maxRetriesCount = %v, cbName = %v: %w", maxRetriesCount, cbName, resultError)
	}

	return nil
}

// NewConnectionProvider ...
func NewConnectionProvider(host string, port int, user, password, vHost string) (*ConnectionProvider, error) {
	if host == "" {
		return nil, errors.New("NewConnectionProvider() host must be not empty")
	}

	amqpURL, err := BuildDSN(host, port, user, password, vHost)
	if err != nil {
		return nil, fmt.Errorf("NewConnectionProvider() unable to build AMQP connection url: %w", err)
	}

	return &ConnectionProvider{
		amqpURL: amqpURL,
	}, nil
}
