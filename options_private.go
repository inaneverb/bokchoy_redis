package bokchoy_redis

import (
	"time"

	"github.com/qioalice/ekago/v2/ekalog"
)

type (
	// Options is the Redis Bokchoy's broker options.
	options struct {
		Logger            *ekalog.Logger
		loggerIsPresented bool

		TickInterval time.Duration
	}
)

var (
	defaultOptions *options
)

func (o *options) apply(options []Option) {
	for _, option := range options {
		if option != nil {
			option(o)
		}
	}
}

func initDefaultOptions() {
	defaultOptions = new(options)
	defaultOptions.apply([]Option{
		WithTickInterval(1 * time.Second),
	})
}
