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

package main

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"

	"github.com/qioalice/ekago/v2/ekaerr"
	"github.com/qioalice/ekago/v2/ekasys"

	"github.com/qioalice/bokchoy"
	"github.com/qioalice/bokchoy_redis/examples/shared"

	"github.com/davecgh/go-spew/spew"
)

func main() {
	const s = "BokchoyExample: Failed to run. "

	// Lambda for disabling echo and buffering
	// https://gist.github.com/mrnugget/9582788
	f := func(arg string) error {
		cmd := exec.Command("stty", arg)
		cmd.Stdin, cmd.Stdout, cmd.Stderr = os.Stdin, os.Stdout, os.Stderr
		return cmd.Run()
	}
	_ = f("cbreak") // disable buffering
	_ = f("-echo")  // disable echo

	shared.TestQueue.OnStart(onStartCallback)
	shared.TestQueue.OnSuccess(onSucceededCallback)
	shared.TestQueue.OnFailure(onFailureCallback)
	shared.TestQueue.OnComplete(onCompleteCallback)
	shared.TestQueue.Use(handler)

	if err := bokchoy.Run(); err.IsNotNil() {
		err.LogAsFatalww(s, nil)
	}
}

func onStartCallback(task *bokchoy.Task) *ekaerr.Error {
	var b bytes.Buffer
	b.WriteString(fmt.Sprintln("It's [onStart] callback."))
	b.WriteString(fmt.Sprintln(">  Task ID: ", task.ID()))
	b.WriteString(fmt.Sprintln(">  Payload dump: "))
	spew.Fdump(&b, task.Payload)
	b.WriteByte('\n')
	_, _ = ekasys.Stdout().Write(b.Bytes())
	return nil
}

func onSucceededCallback(task *bokchoy.Task) *ekaerr.Error {
	var b bytes.Buffer
	b.WriteString(fmt.Sprintln("It's [onSuccess] callback."))
	b.WriteString(fmt.Sprintln(">  Task ID: ", task.ID()))
	b.WriteString(fmt.Sprintln(">  Payload dump: "))
	spew.Fdump(&b, task.Payload)
	b.WriteByte('\n')
	_, _ = ekasys.Stdout().Write(b.Bytes())
	return nil
}

func onFailureCallback(task *bokchoy.Task) *ekaerr.Error {
	var b bytes.Buffer
	b.WriteString(fmt.Sprintln("It's [onFailure] callback."))
	b.WriteString(fmt.Sprintln(">  Retry number: ", task.MaxRetries))
	b.WriteString(fmt.Sprintln(">  Task ID: ", task.ID()))
	b.WriteString(fmt.Sprintln(">  Payload dump: "))
	spew.Fdump(&b, task.Payload)
	b.WriteByte('\n')
	_, _ = ekasys.Stdout().Write(b.Bytes())
	return nil
}

func onCompleteCallback(task *bokchoy.Task) *ekaerr.Error {
	var b bytes.Buffer
	b.WriteString(fmt.Sprintln("It's [onComplete] callback."))
	b.WriteString(fmt.Sprintln(">  Task ID: ", task.ID()))
	b.WriteString(fmt.Sprintln(">  Payload dump: "))
	spew.Fdump(&b, task.Payload)
	b.WriteByte('\n')
	_, _ = ekasys.Stdout().Write(b.Bytes())
	return nil
}

func handler(task *bokchoy.Task) *ekaerr.Error {
	var b bytes.Buffer
	b.WriteString(fmt.Sprintln("HANDLER"))
	b.WriteString(fmt.Sprintln(">  Task ID: ", task.ID()))
	b.WriteString(fmt.Sprintln(">  Payload dump: "))
	spew.Fdump(&b, task.Payload)
	b.WriteByte('\n')
	_, _ = ekasys.Stdout().Write(b.Bytes())
	return nil
}
