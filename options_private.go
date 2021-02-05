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

	"github.com/qioalice/ekago/v2/ekalog"

	"github.com/go-redis/redis/v7"
)

type (
	// Options is the Redis Bokchoy's broker options.
	options struct {

		// --- Required options ---

		Client redis.UniversalClient

		// --- Additional options ---

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
