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
//

package shared

import (
	"context"
	"fmt"
	"time"

	"github.com/qioalice/ekago/v3/ekaerr"
	"github.com/qioalice/ekago/v3/ekalog"
	"github.com/qioalice/ekago/v3/ekatime"

	"github.com/qioalice/bokchoy"
	"github.com/qioalice/bokchoy_redis"

	"github.com/mediocregopher/radix/v4"
)

type (
	UserDefinedPayloadType struct {
		Text      string
		Timestamp ekatime.Timestamp
	}
)

//goland:noinspection GoSnakeCaseUsage
const (
	LISTEN_NETWORK = "tcp"
	LISTEN_ADDR    = "127.0.0.1:6379"
	SELECT_DB      = "12"
)

var (
	TestQueue *bokchoy.Queue
)

func init() {
	const s = "BokchoyExample: Failed to initialize. "

	fmt.Println()

	c, legacyErr := radix.PoolConfig{Dialer: radix.Dialer{SelectDB: SELECT_DB}}.
		New(context.Background(), LISTEN_NETWORK, LISTEN_ADDR)

	err := ekaerr.InitializationFailed.Wrap(legacyErr, s).WithString("redis_addr", LISTEN_ADDR)
	ekalog.Emerge("", err)

	bokchoyRedisBroker, err := bokchoy_redis.NewBroker(
		bokchoy_redis.WithRedisClient(c),
	)
	ekalog.Emerge("", err)

	err = bokchoy.Init(
		bokchoy.WithBroker(bokchoyRedisBroker),
		bokchoy.WithCustomSerializerJSON(UserDefinedPayloadType{}),
		bokchoy.WithRetryIntervals([]time.Duration{
			2 * time.Second,
			4 * time.Second,
			10 * time.Second,
		}),
	)
	ekalog.Emerge("", err)

	TestQueue = bokchoy.GetQueue("test-queue")
}
