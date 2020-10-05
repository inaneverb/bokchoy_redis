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
	"strconv"
	"strings"
	"time"

	"github.com/qioalice/ekago/v2/ekaerr"
	"github.com/qioalice/ekago/v2/ekatime"

	"github.com/qioalice/bokchoy"

	"github.com/go-redis/redis/v7"
)

// buildKey builds key for Redis values, like:
// "bokchoy/<part1>/<part2>" if both of 'part1', 'part2' are presented and not empty,
// or "bokchoy/<part1>", "bokchoy//<part2>" if only one of 'part1', 'part2' is presented.
func (_ *RedisBroker) buildKey(part1, part2 string) string {
	return bokchoy.BuildKey("bokchoy", part1, part2)
}

func (p *RedisBroker) getMany(taskKeys []string) ([][]byte, *ekaerr.Error) {
	const s = "Bokchoy: Failed to get many tasks by its keys. "

	encodedTasks, legacyErr := p.client.MGet(taskKeys...).Result()
	if legacyErr != nil {
		return nil, ekaerr.ExternalError.
			Wrap(legacyErr, s).
			AddFields(
				"bokchoy_task_keys", strings.Join(taskKeys, ", "),
				"bokchoy_error_redis_command", "MGET").
			Throw()
	}

	ret := make([][]byte, len(encodedTasks))
	for i, n := 0, len(encodedTasks); i < n; i++ {
		ret[i] = []byte(encodedTasks[i].(string))
	}

	return ret, nil
}

func (p *RedisBroker) consumeDelayed(queueName string, tickInterval time.Duration) {
	p.mu.Lock()
	defer p.mu.Unlock()

	consumeDelayedWorkerEach := func(
		p *RedisBroker,
		delayedQueueName,
		originalQueueName string,
		tickInterval time.Duration,
	) {
		ticker := time.NewTicker(tickInterval)
		for range ticker.C {
			continue_ := p.consumeDelayedWorker(originalQueueName, delayedQueueName)
			if !continue_ {
				return
			}
		}
	}

	delayedQueueName := queueName + ":delay"
	_, consumeWorkerAlreadyRunning := p.qcd[delayedQueueName]

	if !consumeWorkerAlreadyRunning {
		go consumeDelayedWorkerEach(p, delayedQueueName, queueName, tickInterval)
		p.qcd[delayedQueueName] = struct{}{}
	}
}

func (p *RedisBroker) consumeDelayedWorker(

	originalQueueName,
	delayedQueueName string,
) (
	continue_ bool,
) {
	maxEta := ekatime.Now()

	encodedTasks, err := p.consume(delayedQueueName, originalQueueName, maxEta)
	if err.IsNotNil() && p.logger.IsValid() {
		err.LogAsErrorwUsing(p.logger,
			"Bokchoy: Failed to retrieve delayed payloads (consume)")
	}

	if len(encodedTasks) == 0 {
		return true
	}

	tasks := make([]bokchoy.Task, len(encodedTasks))
	for i, n := 0, len(encodedTasks); i < n; i++ {
		err := tasks[i].Deserialize(encodedTasks[i], bokchoy.DefaultSerializerDummy())
		if err.IsNotNil() {
			panic(err) // todo
		}
	}

	_, legacyErr := p.client.TxPipelined(func(pipe redis.Pipeliner) error {
		for i, encodedTask := range encodedTasks {
			taskID := tasks[i].ID()

			err := p.publish(pipe, originalQueueName, taskID, encodedTask, 0)
			if err != nil {
				err.
					AddMessage("Bokchoy: Failed to republish delayed tasks.").
					AddFields(
						"bokchoy_republished_before_error", i,
						"bokchoy_republished_to_be", len(encodedTasks)).
					Throw()
				return wrapEkaerr(err)
			}
		}

		// To avoid data loss, we only remove the range when results are processed
		legacyErr := pipe.ZRemRangeByScore(
			delayedQueueName,
			"0",
			strconv.FormatInt(maxEta.I64(), 10),
		).Err()
		if legacyErr != nil {
			return legacyErr
		}

		return nil
	})

	if legacyErr != nil {
		if err = extractEkaerr(legacyErr); err.IsNil() {
			// Not *ekaerr.Error, then it's:
			// Redis client's Exec() func error (called in TxPipelined())
			// or pipe.ZRemRangeByScore()'s one.
			err = ekaerr.Interrupted.
				Wrap(legacyErr, "Bokchoy: Failed to republish delayed tasks.")
		}
	}

	//goland:noinspection GoNilness, cause IsNotNil() call is nil safe.
	if err.IsNotNil() && p.logger.IsValid() {
		err.
			AddFields(
				"bokchoy_queue_name", originalQueueName,
				"bokchoy_queue_name_delayed", delayedQueueName).
			LogAsErrorwwUsing(p.logger,
				"Failed to consume delayed tasks.", nil)
	}

	return true
}

func (p *RedisBroker) consume(

	queueName string,
	taskPrefix string,
	eta ekatime.Timestamp,
) (
	[][]byte,
	*ekaerr.Error,
) {
	//const s = "failee"fe3f3fe
	// todo: remove queue name from log only keys
	var (
		result   []string
		queueKey = p.buildKey(queueName, "")
	)

	if eta == 0 {
		p.consumeDelayed(queueName, 1*time.Second)

		result_, legacyErr := p.client.BRPop(1*time.Second, queueKey).Result()
		if legacyErr != nil && legacyErr != redis.Nil {
			return nil, ekaerr.ExternalError.
				Wrap(legacyErr, "Bokchoy: Failed to retrieve payloads (consume)").
				AddFields(
					"bokchoy_queue_key", queueKey,
					"bokchoy_queue_name", queueName,
					"bokchoy_task_prefix", taskPrefix,
					"bokchoy_error_redis_command", "BRPOP").
				Throw()
		}

		if len(result_) > 0 {
			// result[0] is the queue key
			// See returned results here: https://redis.io/commands/brpop
			result = result_[1:]
		}

	} else {

		results := p.client.ZRangeByScore(queueKey, &redis.ZRangeBy{
			Min: "0",
			Max: strconv.FormatInt(eta.I64(), 10),
		})

		if legacyErr := results.Err(); legacyErr != nil && legacyErr != redis.Nil {
			return nil, ekaerr.ExternalError.
				Wrap(legacyErr, "Bokchoy: Failed to retrieve payloads (consume)").
				AddFields(
					"bokchoy_queue_key", queueKey,
					"bokchoy_queue_name", queueName,
					"bokchoy_task_prefix", taskPrefix,
					"bokchoy_error_redis_command", "ZRANGEBYSCORE").
				Throw()
		}

		result = results.Val()
	}

	if len(result) == 0 {
		return nil, nil
	}

	taskKeys := make([]string, 0, len(result))
	for i, n := 0, len(result); i < n; i++ {
		if result[i] == queueName {
			continue
		}

		taskKeys = append(taskKeys, p.buildKey(taskPrefix, result[i]))
	}

	encodedTasks, err := p.getMany(taskKeys)
	if err.IsNotNil() {
		return nil, err.
			AddMessage("Bokchoy: Failed to retrieve payloads (consume)").
			AddFields(
				"bokchoy_queue_key", queueKey,
				"bokchoy_queue_name", queueName,
				"bokchoy_task_prefix", taskPrefix).
			Throw()
	}

	return encodedTasks, nil
}

func (p *RedisBroker) publish(

	client redis.Cmdable,
	queueName,
	taskID string,
	data []byte,
	eta ekatime.Timestamp,

) *ekaerr.Error {

	prefixedTaskKey := p.buildKey(queueName, taskID)

	legacyErr := client.Set(prefixedTaskKey, data, 0).Err()
	if legacyErr != nil {
		return ekaerr.ExternalError.
			Wrap(legacyErr, "Bokchoy: Failed to publish task. "+
				"Failed to save payload of task").
			AddFields(
				"bokchoy_task_key", prefixedTaskKey,
				"bokchoy_task_id", taskID,
				"bokchoy_queue_name", queueName,
				"bokchoy_error_redis_command", "SET").
			Throw()
	}

	if eta == 0 {

		legacyErr = client.RPush(p.buildKey(queueName, ""), taskID).Err()
		if legacyErr != nil {
			return ekaerr.ExternalError.
				Wrap(legacyErr, "Bokchoy: Failed to publish task. "+
					"Failed to add task to the queue").
				AddFields(
					"bokchoy_task_key", prefixedTaskKey,
					"bokchoy_task_id", taskID,
					"bokchoy_queue_name", queueName,
					"bokchoy_error_redis_command", "RPUSH").
				Throw()
		}

	} else if now := ekatime.Now(); eta <= now {
		// if eta is before now, then we should push this taskID in priority

		legacyErr = client.LPush(p.buildKey(queueName, ""), taskID).Err()
		if legacyErr != nil {
			return ekaerr.ExternalError.
				Wrap(legacyErr, "Bokchoy: Failed to publish task. "+
					"Failed to add task to the queue").
				AddFields(
					"bokchoy_task_key", prefixedTaskKey,
					"bokchoy_task_id", taskID,
					"bokchoy_queue_name", queueName,
					"bokchoy_error_redis_command", "LPUSH").
				Throw()
		}

	} else {

		legacyErr = client.ZAdd(p.buildKey(queueName+":delay", ""), &redis.Z{
			Score:  float64(now),
			Member: taskID,
		}).Err()
		if legacyErr != nil {
			return ekaerr.ExternalError.
				Wrap(legacyErr, "Bokchoy: Failed to publish task. "+
					"Failed to add task to the queue").
				AddFields(
					"bokchoy_task_key", prefixedTaskKey,
					"bokchoy_task_id", taskID,
					"bokchoy_queue_name", queueName,
					"bokchoy_error_redis_command", "ZADD").
				Throw()
		}
	}

	return nil
}
