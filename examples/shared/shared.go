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

	"github.com/qioalice/ekago/v2/ekaerr"

	"github.com/qioalice/bokchoy"
	"github.com/qioalice/bokchoy_redis"

	"github.com/go-redis/redis/v7"
)

const DSN = `redis://127.0.0.1:6379/14`

var TestQueue *bokchoy.Queue

func init() {
	const s = "BokchoyExample: Failed to initialize. "

	fmt.Println()

	redisOptions, legacyErr := redis.ParseURL(DSN)
	ekaerr.InitializationFailed.
		Wrap(legacyErr, s+"Incorrect DSN.").
		AddFields("bokchoy_example_incorrect_dsn", DSN).
		LogAsFatalww(s, nil)

	redisClient := redis.NewClient(redisOptions)
	bokchoyRedisBroker := bokchoy_redis.NewBroker(redisClient)

	bokchoy.Init(
		bokchoy.WithBroker(bokchoyRedisBroker),
		bokchoy.WithSerializer(bokchoy.DefaultSerializerJSON()),
	)

	TestQueue = bokchoy.GetQueue("test-queue")
}
