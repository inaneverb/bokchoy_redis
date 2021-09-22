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

package bokchoy_redis

import (
	"unsafe"
)

func buildKey1(p1 string) string {
	return "bokchoy/" + p1
}

func buildKey2(p1, p2 string) string {
	buf := make([]byte, 0, 9+len(p1)+len(p2))
	buf = append(buf, "bokchoy/"...)
	buf = append(buf, p1...)
	buf = append(buf, '/')
	buf = append(buf, p2...)
	return *(*string)(unsafe.Pointer(&buf))
}
