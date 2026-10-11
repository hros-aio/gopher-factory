package eventmanager

import (
	"context"
	"github.com/stretchr/testify/require"
	"testing"
)

type compileSubscriber struct{}

func (compileSubscriber) Register(string, Handler) error { return nil }
func (compileSubscriber) Run(context.Context) error      { return nil }
func (compileSubscriber) Ping(context.Context) error     { return nil }
func (compileSubscriber) Close(context.Context) error    { return nil }

var _ Subscriber = compileSubscriber{}

func TestChainOrder(t *testing.T) {
	var got []string
	wrap := func(n string) Middleware {
		return func(h Handler) Handler {
			return func(ctx context.Context, m Message) error { got = append(got, n); return h(ctx, m) }
		}
	}
	h := Chain(func(context.Context, Message) error { got = append(got, "handler"); return nil }, wrap("first"), wrap("second"))
	require.NoError(t, h(context.Background(), Message{}))
	require.Equal(t, []string{"first", "second", "handler"}, got)
}
