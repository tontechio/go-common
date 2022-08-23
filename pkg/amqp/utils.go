package amqp

import (
	"errors"
	"fmt"
)

// BuildDSN ...
func BuildDSN(host string, port int, user, password, vHost string) (string, error) {
	if host == "" {
		return "", errors.New("host must be not empty")
	}

	if user == "" {
		return "", errors.New("user must be not empty")
	}

	if password == "" {
		return "", errors.New("password must be not empty")
	}

	if port <= 0 || port > 65535 {
		return "", errors.New("port must be between 0 and 65535")
	}

	resultVhost := "/"

	if vHost == "" {
		resultVhost = vHost
	}

	return fmt.Sprintf("amqp://%s:%s@%s:%v%v", user, password, host, port, resultVhost), nil
}
