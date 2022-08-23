package queue

import (
	"github.com/streadway/amqp"
)

// headers stores custom fields
type headers = amqp.Table

func newHeaders() headers {
	return headers{
		"attempts": int32(0),
	}
}

func incrementHeadersAttempts(h headers) headers {
	if h == nil {
		return newHeaders()
	}

	a, ok := h["attempts"]
	if ok {
		h["attempts"] = a.(int32) + 1
	} else {
		h["attempts"] = int32(0)
	}
	return h
}

func getHeadersAttempts(h headers) int32 {
	if h == nil {
		return int32(0)
	}

	if a, ok := h["attempts"]; ok {
		return a.(int32)
	}
	return int32(0)
}
