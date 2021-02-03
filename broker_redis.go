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
	"fmt"
	"strings"
	"sync"
	"time"
	"unsafe"

	"github.com/qioalice/ekago/v2/ekaerr"
	"github.com/qioalice/ekago/v2/ekalog"
	"github.com/qioalice/ekago/v2/ekaunsafe"

	"github.com/qioalice/bokchoy"

	"github.com/go-redis/redis/v7"
)

type (
	// RedisBroker is the redis broker.
	RedisBroker struct {
		client     redis.UniversalClient
		clientInfo string
		logger     *ekalog.Logger
		scripts    map[string]string
		mu         *sync.Mutex
		qcd        map[string]struct{} // queues must be consumed delayed
	}
)

var _ bokchoy.Broker = (*RedisBroker)(nil)

func NewBroker(clt redis.UniversalClient, options ...Option) *RedisBroker {

	defaultOptionsCopy := *defaultOptions
	optionsObject := &defaultOptionsCopy

	for i, n := 0, len(options); i < n; i++ {
		if options[i] != nil {
			options[i](optionsObject)
		}
	}

	return NewBrokerCustomLogger(clt, ekalog.With())
}

// NewRedisBroker initializes a new redis broker instance.
func NewBrokerCustomLogger(clt redis.UniversalClient, logger *ekalog.Logger) *RedisBroker {

	clientInfo := "<Incorrect Redis client>"
	if clt != nil && ekaunsafe.TakeRealAddr(clt) != nil {

		// Go to options and extract address.
		// C-style magic.
		// Let's gonna be dirty!
		//
		// So, each Redis' client contains its private part by pointer,
		// which contains its options by pointer, like:
		//
		//    type SomeRedisClient struct {
		//        *someRedisClientPrivate
		//        ...
		//    }
		//    type someRedisClientPrivate struct {
		//        *someRedisClientPrivateOptions
		//        ...
		//    }
		//    type someRedisClientPrivateOptions struct {
		//        < that's what we need >
		//    }
		//
		// So, it's not hard.
		fn := func(ptr unsafe.Pointer, typ int) string {
			// ptr is either *redis.Client, *redis.ClusterClient or *redis.Ring
			// we have to dereference it, and then we'll receive a pointer
			// to the either *redis.baseClient, *redis.clusterClient or *redis.ring
			doublePtr := (*unsafe.Pointer)(ptr)
			ptr = *doublePtr
			// once more to get options
			doublePtr = (*unsafe.Pointer)(ptr)
			ptr = *doublePtr
			// and that's it

			switch typ {
			case 1:
				options := (*redis.Options)(ptr)
				return fmt.Sprintf("Redis client, [%s], %d DB",
					options.Addr, options.DB)

			case 2:
				options := (*redis.ClusterOptions)(ptr)
				return fmt.Sprintf("Redis cluster, [%s]",
					strings.Join(options.Addrs, ", "))

			case 3:
				options := (*redis.RingOptions)(ptr)
				addresses := make([]string, 0, len(options.Addrs))
				for _, addr := range options.Addrs {
					addresses = append(addresses, addr)
				}
				return fmt.Sprintf("Redis ring, [%s], %d DB",
					strings.Join(addresses, ", "), options.DB)

			default:
				return "<Unknown Redis client>"
			}
		}

		switch client := clt.(type) {
		case *redis.Client:
			clientInfo = fn(unsafe.Pointer(client), 1)
		case *redis.ClusterClient:
			clientInfo = fn(unsafe.Pointer(client), 2)
		case *redis.Ring:
			clientInfo = fn(unsafe.Pointer(client), 3)
		}
	}

	return &RedisBroker{
		client:     clt,
		clientInfo: clientInfo,
		logger:     logger,
		qcd:        make(map[string]struct{}),
		mu:         &sync.Mutex{},
	}
}

func (p *RedisBroker) String() string {
	if p == nil {
		return "<Incorrect Redis client>"
	}
	return p.clientInfo
}

// Initialize initializes the redis broker.
func (p *RedisBroker) Initialize() *ekaerr.Error {

	if err := p.Ping(); err.IsNotNil() {
		return err.
			Throw()
	}

	return nil
}

// Ping pings the redis broker to ensure it's well connected.
func (p RedisBroker) Ping() *ekaerr.Error {

	if legacyErr := p.client.Ping().Err(); legacyErr != nil {
		return ekaerr.ExternalError.
			Wrap(legacyErr, "Bokchoy: Failed to ping Redis server").
			AddFields("bokchoy_broker", p.clientInfo).
			Throw()
	}

	return nil
}

// Consume returns an array of raw data.
func (p *RedisBroker) Consume(name string, etaUnixNano int64) ([][]byte, *ekaerr.Error) {
	encodedTasks, err := p.consume(name, name, etaUnixNano)
	return encodedTasks, err.
		Throw()
}

// Get returns stored raw data from task key.
func (p *RedisBroker) Get(taskKey string) ([]byte, *ekaerr.Error) {
	taskKey = p.buildKey(taskKey, "")

	res, legacyErr := p.client.Get(taskKey).Bytes()
	if legacyErr != nil {
		return nil, ekaerr.ExternalError.
			Wrap(legacyErr, "Bokchoy: Failed to get task").
			AddFields(
				"bokchoy_task_key", taskKey,
				"bokchoy_error_redis_command", "GET").
			Throw()
	}

	return res, nil
}

// Delete deletes raw data in broker based on key.
func (p *RedisBroker) Delete(queueName, taskID string) *ekaerr.Error {
	prefixedTaskKey := p.buildKey(queueName, taskID)

	_, legacyErr := p.client.Del(prefixedTaskKey).Result()
	if legacyErr != nil {
		return ekaerr.ExternalError.
			Wrap(legacyErr, "Bokchoy: Failed to delete task").
			AddFields(
				"bokchoy_task_key", prefixedTaskKey,
				"bokchoy_task_id", taskID,
				"bokchoy_error_redis_command", "DEL").
			Throw()
	}

	return nil
}

func (p *RedisBroker) List(queueName string) ([][]byte, *ekaerr.Error) {
	const s = "Bokchoy: Failed to get tasks from queue. "

	taskIDs, legacyErr := p.client.LRange(queueName, 0, -1).Result()
	if legacyErr != nil {
		return nil, ekaerr.ExternalError.
			Wrap(legacyErr, s).
			AddFields(
				"bokchoy_queue_name", queueName,
				"bokchoy_error_redis_command", "LRANGE").
			Throw()
	}

	for i, n := 0, len(taskIDs); i < n; i++ {
		taskIDs[i] = p.buildKey(queueName, taskIDs[i])
	}

	payloads, err := p.getMany(taskIDs)
	if err.IsNotNil() {
		return nil, err.
			AddMessage(s).
			AddFields("bokchoy_queue_name", queueName).
			Throw()
	}

	return payloads, nil
}

// Count returns number of items from a queue name.
func (p *RedisBroker) Count(queueName string) (bokchoy.BrokerStats, *ekaerr.Error) {

	var stats bokchoy.BrokerStats
	queueName = p.buildKey(queueName, "")

	direct, legacyErr := p.client.LLen(queueName).Result()
	if legacyErr != nil && legacyErr != redis.Nil {
		return stats, ekaerr.ExternalError.
			Wrap(legacyErr, "Bokchoy: Failed to get a queue stat").
			AddFields(
				"bokchoy_queue_name", queueName,
				"bokchoy_error_redis_command", "LLEN").
			Throw()
	}

	delayed, legacyErr := p.client.ZCount(queueName+":delay", "-inf", "+inf").Result()
	if legacyErr != nil && legacyErr != redis.Nil {
		return stats, ekaerr.ExternalError.
			Wrap(legacyErr, "Bokchoy: Failed to get a queue stat").
			AddFields(
				"bokchoy_queue_name", queueName,
				"bokchoy_error_redis_command", "ZCOUNT").
			Throw()
	}

	stats.Direct = int(direct)
	stats.Delayed = int(delayed)
	stats.Total = stats.Direct + stats.Delayed

	return stats, nil
}

// Save synchronizes the stored item in redis.
func (p *RedisBroker) Set(

	taskKey string,
	data []byte,
	expiration time.Duration,

) *ekaerr.Error {

	taskKey = p.buildKey(taskKey, "")

	legacyErr := p.client.Set(taskKey, data, expiration).Err()
	if legacyErr != nil {
		return ekaerr.ExternalError.
			Wrap(legacyErr, "Bokchoy: Failed to sync task").
			AddFields(
				"bokchoy_task_key", taskKey,
				"bokchoy_error_redis_command", "SET").
			Throw()
	}

	return nil
}

// Publish publishes raw data.
// it uses a hash to store the task itself
// pushes the task id to the list or a zset if the task is delayed.
func (p *RedisBroker) Publish(

	queueName,
	taskID string,
	data []byte,
	eta int64,

) *ekaerr.Error {

	_, legacyErr := p.client.Pipelined(func(pipe redis.Pipeliner) error {
		err := p.publish(pipe, queueName, taskID, data, eta)
		return wrapEkaerr(err.Throw())
	})

	var err *ekaerr.Error

	if err = extractEkaerr(legacyErr); err.IsNil() && legacyErr != nil {
		err = ekaerr.ExternalError.
			Wrap(legacyErr, "Bokchoy: Failed to publish task")
	}

	//goland:noinspection GoNilness
	if err.IsNotNil() {
		return err.
			AddFields(
				"bokchoy_task_id", taskID,
				"bokchoy_queue_name", queueName).
			Throw()
	}

	return nil
}

// Empty removes the redis key for a queue.
func (p *RedisBroker) Empty(queueName string) *ekaerr.Error {
	const s = "Failed to empty queue using Redis as backend. "

	queueKey := p.buildKey(queueName, "")

	legacyErr := p.client.Del(queueName).Err()
	if legacyErr != nil && legacyErr != redis.Nil {
		return ekaerr.ExternalError.
			Wrap(legacyErr, s).
			AddFields(
				"bokchoy_queue_name", queueName,
				"bokchoy_queue_key", queueKey,
				"bokchoy_error_redis_command", "DEL").
			Throw()
	}

	return nil
}

// ClearAll removes the whole Redis database.
//
// WARNING! ABSOLUTELY ALL DATA WILL BE WIPED!
// NOT ONLY BOKCHOY'S BUT ABSOLUTELY ALL!
func (p *RedisBroker) ClearAll() *ekaerr.Error {

	// TODO: Replace FLUSHDB -> DEL of KEYS (remove only Bokchoy's data)

	if legacyErr := p.client.FlushDB().Err(); legacyErr != nil {
		return ekaerr.ExternalError.
			Wrap(legacyErr, "Bokchoy: Failed to wipe whole database").
			Throw()
	}

	return nil
}
