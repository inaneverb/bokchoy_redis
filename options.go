package bokchoy_redis

import (
	"time"

	"github.com/qioalice/ekago/v2/ekalog"
)

// Option is an option unit.
type (
	Option func(opts *options)
)

// WithLogger defines the Logger.
func WithLogger(logger *ekalog.Logger) Option {
	return func(opts *options) {
		opts.Logger = logger
		opts.loggerIsPresented = true
	}
}

// WithTickInterval determines with which interval RedisBroker.Consume()
// will return a new bulk of received tasks to consume.
//
// Acceptable values: [1 * time.Microsecond .. 24 * 365 * time.Hour].
func WithTickInterval(tick time.Duration) Option {
	//goland:noinspection GoSnakeCaseUsage
	const (
		LOWER_BOUND = 1 * time.Microsecond
		UPPER_BOUND = 24 * 365 * time.Hour
	)
	return func(opts *options) {
		if LOWER_BOUND <= tick && tick <= UPPER_BOUND {
			opts.TickInterval = tick
		}
	}
}
