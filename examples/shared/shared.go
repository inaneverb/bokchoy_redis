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
	"fmt"
	"time"

	"github.com/qioalice/ekago/v2/ekaerr"
	"github.com/qioalice/ekago/v2/ekatime"

	"github.com/qioalice/bokchoy"
	"github.com/qioalice/bokchoy_redis"

	"github.com/go-redis/redis/v7"
	"github.com/modern-go/reflect2"
)

const (
	DSN = `redis://default:root@127.0.0.1:6379/14`
)

type (
	UserDefinedPayloadType struct {
		Text      string
		Timestamp ekatime.Timestamp
	}
)

var (
	TestQueue *bokchoy.Queue
)

func init() {
	const s = "BokchoyExample: Failed to initialize. "

	fmt.Println()

	redisOptions, legacyErr := redis.ParseURL(DSN)
	ekaerr.InitializationFailed.
		Wrap(legacyErr, s+"Incorrect DSN.").
		AddFields("bokchoy_example_incorrect_dsn", DSN).
		LogAsFatal(s)

	redisClient := redis.NewClient(redisOptions)
	bokchoyRedisBroker := bokchoy_redis.NewBroker(redisClient)

	payloadDesiredType := reflect2.TypeOf(new(UserDefinedPayloadType))
	jsonSerializer := bokchoy.CustomSerializerJSON(payloadDesiredType)

	bokchoy.Init(
		bokchoy.WithBroker(bokchoyRedisBroker),
		bokchoy.WithSerializer(jsonSerializer),
		bokchoy.WithRetryIntervals([]time.Duration{
			2 * time.Second,
			4 * time.Second,
			10 * time.Second,
		}),
	).
		LogAsFatal(s)

	TestQueue = bokchoy.GetQueue("test-queue")
}
