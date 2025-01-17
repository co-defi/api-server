package common

import "context"

type CommandHandler[C any] interface {
	Handle(ctx context.Context, cmd C) (string, error)
}
