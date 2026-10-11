package eventmanager

import (
	"fmt"
	"regexp"
)

var topicComponent = regexp.MustCompile(`^[a-z][a-z0-9_-]*$`)
var topicName = regexp.MustCompile(`^[a-z][a-z0-9_-]*\.[a-z][a-z0-9_-]*\.v[1-9][0-9]*$`)

// Topic builds a versioned topic name.
func Topic(domain, event string, version int) (string, error) {
	if !topicComponent.MatchString(domain) || !topicComponent.MatchString(event) || version <= 0 {
		return "", fmt.Errorf("%w: invalid topic components", ErrInvalidConfig)
	}
	return fmt.Sprintf("%s.%s.v%d", domain, event, version), nil
}

// ValidateTopic checks the standard topic naming convention.
func ValidateTopic(topic string) error {
	if !topicName.MatchString(topic) {
		return fmt.Errorf("%w: invalid topic %q", ErrInvalidConfig, topic)
	}
	return nil
}

// MessageKey builds a tenant-scoped aggregate key. Empty tenant means global.
func MessageKey(tenantID, aggregateID string) (string, error) {
	if aggregateID == "" {
		return "", fmt.Errorf("%w: aggregate ID required", ErrInvalidConfig)
	}
	return tenantID + ":" + aggregateID, nil
}
