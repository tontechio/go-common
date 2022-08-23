package main

import (
	"fmt"

	"github.com/tontechio/go-common/pkg/queue"
)

type handler struct {
	producer *queue.Producer
}

// HandleMessage ...
func (h *handler) HandleMessage(msg []byte) error {
	return h.producer.Emit(msg)
}

func newHandler(
	producer *queue.Producer,
) (*handler, error) {
	if producer == nil {
		return nil, fmt.Errorf("empty producer")
	}
	return &handler{
		producer: producer,
	}, nil
}
