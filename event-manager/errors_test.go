package eventmanager

import (
	"errors"
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestClassification(t *testing.T) {
	base := errors.New("base")
	r := fmt.Errorf("outer: %w", MarkRetryable(base))
	assert.True(t, IsRetryable(r))
	assert.ErrorIs(t, r, base)
	p := MarkPermanent(base)
	assert.False(t, IsRetryable(p))
	assert.ErrorIs(t, p, base)
}
func TestNilClassification(t *testing.T) {
	assert.Nil(t, MarkRetryable(nil))
	assert.Nil(t, MarkPermanent(nil))
	assert.False(t, IsRetryable(errors.New("plain")))
}
