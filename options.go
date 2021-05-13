//
// ORIGINAL PACKAGE
// ( https://github.com/thoas/bokchoy )
//
//     Copyright © 2019. All rights reserved.
//     Author: Florent Messa
//     Contacts: florent.messa@gmail.com, https://github.com/thoas
//     License: https://opensource.org/licenses/MIT
//
// HAS BEEN FORKED, HIGHLY MODIFIED AND NOW IS AVAILABLE AS
// ( https://github.com/qioalice/bokchoy )
//
//     Copyright © 2020. All rights reserved.
//     Author: Ilya Stroy.
//     Contacts: qioalice@gmail.com, https://github.com/qioalice
//     License: https://opensource.org/licenses/MIT

package bokchoy_redis

import (
	"time"

	"github.com/qioalice/ekago/v3/ekalog"

	"github.com/mediocregopher/radix/v4"
)

// Option is an option unit.
type (
	Option func(opts *options)
)

// WithRedisClient defines Redis client that will be used as Bokchoy's broker backend.
func WithRedisClient(client radix.Client) Option {
	return func(opts *options) {
		opts.Client = client
	}
}

// WithLogger defines the Logger.
func WithLogger(logger *ekalog.Logger) Option {
	return func(opts *options) {
		opts.Logger = logger
	}
}

// WithTickInterval determines with which interval RedisBroker.Consume()
// will return a new bulk of received tasks to consume.
//
// Acceptable values: [1 * time.Microsecond .. 1 * time.Minute].
func WithTickInterval(tick time.Duration) Option {
	//goland:noinspection GoSnakeCaseUsage
	const (
		LOWER_BOUND = 1 * time.Microsecond
		UPPER_BOUND = 1 * time.Minute
	)
	return func(opts *options) {
		if LOWER_BOUND <= tick && tick <= UPPER_BOUND {
			opts.TickInterval = tick
		}
	}
}
