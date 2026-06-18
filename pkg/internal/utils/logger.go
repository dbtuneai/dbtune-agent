package utils

import "github.com/sirupsen/logrus"

// LeveledLogrus is a wrapper around logrus that provides leveled logging
// This is needed to be compatible with retryablehttp:
// https://github.com/hashicorp/go-retryablehttp/pull/101#issuecomment-735206810
type LeveledLogrus struct {
	*logrus.Logger
}

func (l *LeveledLogrus) fields(keysAndValues ...interface{}) map[string]interface{} {
	fields := make(map[string]interface{})

	for i := 0; i < len(keysAndValues)-1; i += 2 {
		fields[keysAndValues[i].(string)] = keysAndValues[i+1]
	}

	return fields
}

func (l *LeveledLogrus) Error(msg string, keysAndValues ...interface{}) {
	// retryablehttp logs each failed *attempt* at Error level; these are
	// transient and retried, so demote to Info to keep retry noise off the
	// backend log hook. Exhausted-retry failures are returned by Do and logged
	// at Error by the caller (runWithTicker / SendHeartbeat).
	l.WithFields(l.fields(keysAndValues...)).Info(msg)
}

func (l *LeveledLogrus) Info(msg string, keysAndValues ...interface{}) {
	l.WithFields(l.fields(keysAndValues...)).Info(msg)
}
func (l *LeveledLogrus) Debug(msg string, keysAndValues ...interface{}) {
	l.WithFields(l.fields(keysAndValues...)).Debug(msg)
}

func (l *LeveledLogrus) Warn(msg string, keysAndValues ...interface{}) {
	l.WithFields(l.fields(keysAndValues...)).Warn(msg)
}
