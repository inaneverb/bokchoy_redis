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

package bokchoy_redis

import (
	"github.com/qioalice/ekago/v2/ekaerr"
)

type (
	ekaerrWrapped ekaerr.Error
)

func (ew *ekaerrWrapped) Error() string {
	return "It's a wrapped *ekaerr.Error. You must not to see this message. Cast instead."
}

func wrapEkaerr(err *ekaerr.Error) error {
	if err.IsNil() {
		return nil
	} else {
		return (*ekaerrWrapped)(err)
	}
}

func extractEkaerr(legacyErr error) *ekaerr.Error {
	if legacyErr == nil {
		return nil
	}
	if ew, ok := legacyErr.(*ekaerrWrapped); ok && ew != nil {
		return (*ekaerr.Error)(ew)
	}
	return nil
}
