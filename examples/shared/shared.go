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
//     Contacts: iyuryevich@pm.me, https://github.com/qioalice
//     License: https://opensource.org/licenses/MIT
//

package shared

import (
	"time"

	"github.com/qioalice/ekago/v3/ekaerr"
	"github.com/qioalice/ekago/v3/ekalog"
	"github.com/qioalice/ekago/v3/ekatime"

	"github.com/qioalice/bokchoy"
	"github.com/qioalice/bokchoy_redis"

	"github.com/mediocregopher/radix/v3"
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
	LISTEN_ADDR    = "127.0.0.1:6378"
	POOL_SIZE      = 4
	SELECT_DB      = 12
	AUTH_PASSWORD  = ""
)

var (
	TestQueue *bokchoy.Queue
)

func init() {
	const s = "BokchoyExample: Failed to initialize. "

	var dialOpts []radix.DialOpt
	dialOpts = append(dialOpts, radix.DialSelectDB(SELECT_DB))

	//goland:noinspection GoBoolExpressions
	if AUTH_PASSWORD != "" {
		dialOpts = append(dialOpts, radix.DialAuthPass(AUTH_PASSWORD))
	}

	customConnFunc := func(network, addr string) (radix.Conn, error) {
		return radix.Dial(network, addr, dialOpts...)
	}

	c, legacyErr := radix.NewPool(
		LISTEN_NETWORK,
		LISTEN_ADDR,
		POOL_SIZE,
		radix.PoolConnFunc(customConnFunc),
	)

	err := ekaerr.InitializationFailed.Wrap(legacyErr, s).
		WithString("redis_addr", LISTEN_ADDR)
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
