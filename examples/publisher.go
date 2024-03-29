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

package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"

	"github.com/qioalice/ekago/v3/ekalog"
	"github.com/qioalice/ekago/v3/ekatime"

	"github.com/qioalice/bokchoy_redis/examples/shared"
)

func main() {
	reader := bufio.NewReader(os.Stdin)
	for {
		iter(reader)
	}
}

func iter(reader *bufio.Reader) {
	const s = "Bokchoy: Failed to publish a new task. "

	fmt.Print("Enter text: ")
	text, _ := reader.ReadString('\n')
	text = strings.TrimSpace(text)
	fmt.Println()

	if text == "" {
		return
	}

	_, err := shared.TestQueue.Publish(shared.UserDefinedPayloadType{
		Text:      text,
		Timestamp: ekatime.NewTimestampNow(),
	})
	ekalog.Emerge("", err)
}
