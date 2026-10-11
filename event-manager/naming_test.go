package eventmanager

import (
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestNaming(t *testing.T) {
	topic, err := Topic("billing", "paid", 2)
	require.NoError(t, err)
	assert.Equal(t, "billing.paid.v2", topic)
	assert.Error(t, ValidateTopic("bad"))
	_, err = Topic("bad.name", "paid", 1)
	assert.Error(t, err)
	key, err := MessageKey("", "agg")
	require.NoError(t, err)
	assert.Equal(t, ":agg", key)
	_, err = MessageKey("t", "")
	assert.Error(t, err)
}
