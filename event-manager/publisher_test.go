package eventmanager

import (
	"context"
	"github.com/stretchr/testify/require"
	"testing"
)

type compilePublisher struct{}

func (compilePublisher) Publish(context.Context, string, string, Event) error { return nil }
func (compilePublisher) Ping(context.Context) error                           { return nil }
func (compilePublisher) Close(context.Context) error                          { return nil }

var _ Publisher = compilePublisher{}

func TestPublisherContract(t *testing.T) {
	var p Publisher = compilePublisher{}
	require.NoError(t, p.Ping(context.Background()))
}
