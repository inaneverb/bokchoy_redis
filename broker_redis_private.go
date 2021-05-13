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
	"context"
	"time"

	"github.com/qioalice/ekago/v3/ekaerr"
	"github.com/qioalice/ekago/v3/ekastr"

	"github.com/qioalice/bokchoy"

	"github.com/mediocregopher/radix/v4"
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

	var rawResp []string
	if _, err := p.execCmd("MGET", &rawResp, CTX_TIMEOUT_DEFAULT, false, taskKeys...); err.IsNotNil() {
		return nil, err.AddMessage(s).WithArray("bokchoy_task_keys", taskKeys).Throw()
	}

	if len(rawResp) == 0 {
		return nil, nil
	}

	resp := make([][]byte, 0, len(rawResp))
	for i, n := 0, len(rawResp); i < n; i++ {
		if rawResp[i] != "" && rawResp[i] != "(nil)" {
			resp = append(resp, ekastr.S2B(rawResp[i]))
		}
	}

	return resp, nil
}

func (p *RedisBroker) execCmd(
	cmd               string,
	dest              interface{},
	timeout           time.Duration,
	nilOrEmptyAsErr   bool,
	args              ...string,
) (
	isNull            bool,
	err               *ekaerr.Error,
) {
	isNull, err = p.do(cmd, dest, timeout, nilOrEmptyAsErr, args, nil)
	return isNull, err.Throw()
}

func (p *RedisBroker) execFlatCmd(
	cmd               string,
	dest              interface{},
	timeout           time.Duration,
	nilOrEmptyAsErr   bool,
	args              ...interface{},
) (
	isNull            bool,
	err               *ekaerr.Error,
) {
	isNull, err = p.do(cmd, dest, timeout, nilOrEmptyAsErr, nil, args)
	return isNull, err.Throw()
}


func (p *RedisBroker) do(
	cmd               string,
	dest              interface{},
	timeout           time.Duration,
	nilOrEmptyAsErr   bool,
	argsStr           []string,
	args              []interface{},
) (
	isNull            bool,
	err               *ekaerr.Error,
) {
	const s = "Bockhoy.Redis: Failed to execute %s command. "

	var respMaybe interface{}
	if dest != nil {
		respMaybe = &radix.Maybe{ Rcv: dest }
	}

	ctx := context.Background()
	if timeout > 0 {
		var cancelFunc context.CancelFunc
		ctx, cancelFunc = context.WithTimeout(ctx, timeout)
		defer cancelFunc()
	}

	var act radix.Action
	if len(args) > 0 {
		act = radix.FlatCmd(respMaybe, cmd, args...)
	} else {
		act = radix.Cmd(respMaybe, cmd, argsStr...)
	}

	if legacyErr := p.client.Do(ctx, act); legacyErr != nil {
		return true, ekaerr.RejectedOperation.Wrap(legacyErr, s, cmd).Throw()
	}

	isNull = respMaybe != nil &&
		(respMaybe.(*radix.Maybe).Null || respMaybe.(*radix.Maybe).Empty)

	if nilOrEmptyAsErr && isNull {
		return true, ekaerr.IllegalState.
			New(s + "Nil or empty response is returned.", cmd).
			Throw()
	}

	return isNull, nil
}
