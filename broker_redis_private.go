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
	"reflect"
	"strings"
	"unsafe"

	"github.com/qioalice/ekago/v2/ekaerr"

	"github.com/qioalice/bokchoy"
)

//goland:noinspection SpellCheckingInspection,GoSnakeCaseUsage
const (

	// AUXILIARY REDIS FUNCTIONS
	//
	//  - KEYSREM pattern.
	//    Removes all keys with passed pattern.
	//    Returns: {$1, $2}:
	//      $1: How much inner SCAN calls has been passed,
	//      $2: How much keys has been deleted.
	//
	//  - QDPOPTASKS key max
	//

	_RS_KEYSREM = `
local cursor = 0
local calls = 0
local dels = 0
repeat
    local result = redis.call('SCAN', cursor, 'MATCH', ARGV[1])
    calls = calls + 1
    for _,key in ipairs(result[2]) do
        redis.call('UNLINK', key)
        dels = dels + 1
    end
    cursor = tonumber(result[1])
until cursor == 0
return {calls, dels}
`
	_RS_QDPOPTASKS = `
local queueKey = KEYS[1]
local max = ARGV[1]
local results = redis.call('ZRANGEBYSCORE', queueKey..':delay', '0', max)
local length = #results
if length > 0 then
    redis.call('ZREMRANGEBYSCORE', queueKey..':delay', '0', max)
    return results
else
    return nil
end
`
	_RS_QDUPTASKS = `
local queueKey = KEYS[1]
local max = ARGV[1]
local results = redis.call('ZRANGEBYSCORE', queueKey..':delay', '0', max)
local length = #results
if length > 0 then
    redis.call('ZREMRANGEBYSCORE', queueKey..':delay', '0', max)
    redis.call('RPUSH', queueKey, unpack(results))
    return length
else
    return nil
end
`
	_RS_QRPUSHTASK = `
local queueKey = KEYS[1]
local taskId = ARGV[1]
local data = ARGV[2]
redis.call('SET', queueKey..'/'..taskId, data)
redis.call('RPUSH', queueKey, taskId)
return {ok='OK'}
`
	_RS_QLPUSHTASK = `
local queueKey = KEYS[1]
local taskId = ARGV[1]
local data = ARGV[2]
redis.call('SET', queueKey..'/'..taskId, data)
redis.call('LPUSH', queueKey, taskId)
return{ok='OK'}
`
	_RS_QDPUSHTASK = `
local queueKey = KEYS[1]
local taskId = ARGV[1]
local score = ARGV[2]
local data = ARGV[3]
redis.call('SET', queueKey..'/'..taskId, data)
redis.call('ZADD', queueKey..':delay', score, taskId)
return{ok='OK'}
`
	_RS_QSTAT = `
local queueKey = KEYS[1]
local llen = redis.call('LLEN', queueKey)
local zcount = redis.call('ZCOUNT', queueKey..':delay', '-inf', '+inf')
return {llen, zcount}
`
)

// Make sure RedisBroker implements bokchoy.Broker interface.
// Compilation time error otherwise.
var _ bokchoy.Broker = (*RedisBroker)(nil)

// isValid reports whether the current RedisBroker is valid,
// and has been instantiated properly, using its constructor.
func (p *RedisBroker) isValid() bool {
	return p != nil && p.client != nil
}

// getMany tries to retrieve an encoded RAW data of bokchoy.Task s,
// the key of which are passed.
func (p *RedisBroker) getMany(taskKeys []string) ([][]byte, *ekaerr.Error) {
	const s = "Bokchoy.RedisBroker: Failed to get many tasks by its keys. "

	var (
		encodedTasks []interface{}
		legacyErr    error
	)

	switch encodedTasks, legacyErr =
		p.client.MGet(taskKeys...).Result(); {

	case legacyErr != nil:
		return nil, ekaerr.ExternalError.
			Wrap(legacyErr, s).
			AddFields(
				"bokchoy_task_keys", strings.Join(taskKeys, ", "),
				"bokchoy_error_redis_command", "MGET").
			Throw()
	}

	var (
		encodedTasksTyped = make([][]byte, 0, len(encodedTasks))
		encodedTaskDummy  []byte
		hd                = (*reflect.SliceHeader)(unsafe.Pointer(&encodedTaskDummy))
	)
	for _, encodedTask := range encodedTasks {
		switch encodedTaskStr, ok := encodedTask.(string); {

		case encodedTask == nil:
			continue

		case ok:
			hs := (*reflect.StringHeader)(unsafe.Pointer(&encodedTaskStr))
			hd.Data = hs.Data
			hd.Len = hs.Len
			hd.Cap = hs.Len
			encodedTasksTyped = append(encodedTasksTyped, encodedTaskDummy)

		default:
			// Impossible rare case.
			// Maybe this is 3032 year and things were changed?
			return nil, ekaerr.InternalError.
				New(s+"MGET returns unexpected type of values. "+
					"What is the year now?").
				AddFields(
					"bokchoy_task_keys", strings.Join(taskKeys, ", "),
					"bokchoy_encoded_task_type", reflect.TypeOf(encodedTask).String(),
					"bokchoy_error_redis_command", "MGET").
				Throw()
		}
	}

	return encodedTasksTyped, nil
}
