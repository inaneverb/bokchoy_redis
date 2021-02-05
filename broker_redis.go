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

//goland:noinspection SpellCheckingInspection
type (
	// RedisBroker is the redis broker.
	RedisBroker struct {

		// --- Main parts ---

		client     redis.UniversalClient
		clientInfo string

		// --- Additional parts ---

		logger       *ekalog.Logger
		tickInterval time.Duration

		// --- Info about consuming delayed queues ---

		mu  *sync.Mutex
		qcd map[string]struct{} // queues must be consumed delayed

		// Redis helpers.
		// Contains SHA1 of loaded Lua scripts into Redis instance.

		_KEYSREM    string
		_QSTAT      string
		_QDPOPTASKS string
		_QDUPTASKS  string
		_QRPUSHTASK string
		_QLPUSHTASK string
		_QDPUSHTASK string
	}
)

// NewBroker creates, initializes and returns a new RedisBroker instance.
// It uses a passed Option s to determine its behaviour.
//
// There are required options, you have to specify:
//
//  - WithRedisClient():
//    You need to specify Redis client, what will be used as backend.
//
//    Allowed types: redis.Client, redis.ClusterClient, redis.Ring.
//    Be carefully with redis.ClusterClient and redis.Ring.
//    It's not tested properly yet.
//
func NewBroker(options ...Option) (rb *RedisBroker, err *ekaerr.Error) {
	const s = "Bokchoy.RedisBroker: Failed to create a new Broker. "

	defaultOptionsCopy := *defaultOptions
	optionsObject := &defaultOptionsCopy

	for i, n := 0, len(options); i < n; i++ {
		if options[i] != nil {
			options[i](optionsObject)
		}
	}

	// Validate options.
	// Some options are must presented by user.
	switch {

	case ekaunsafe.TakeRealAddr(optionsObject.Client) == nil:
		return nil, ekaerr.InitializationFailed.
			New(s + "Redis client must be presented. " +
				"Use WithRedisClient() option as a part of constructor argument.").
			Throw()
	}

	br := &RedisBroker{
		client:       optionsObject.Client,
		logger:       ekalog.With(),
		tickInterval: optionsObject.TickInterval,
		qcd:          make(map[string]struct{}),
		mu:           &sync.Mutex{},
	}

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

	br.clientInfo = "<Incorrect Redis client>"
	switch client := optionsObject.Client.(type) {

	case *redis.Client:
		br.clientInfo = fn(unsafe.Pointer(client), 1)

	case *redis.ClusterClient:
		br.clientInfo = fn(unsafe.Pointer(client), 2)

	case *redis.Ring:
		br.clientInfo = fn(unsafe.Pointer(client), 3)
	}

	// User can "disable" logging passing nil or invalid logger.
	// Thus there is no either nil check nor logger.IsValid() call.
	if optionsObject.loggerIsPresented {
		br.logger = optionsObject.Logger
	}

	if err = br.Ping(); err.IsNotNil() {
		return nil, err.
			AddMessage(s).
			Throw()
	}

	// Ping is OK, we can load scripts.
	//goland:noinspection SpellCheckingInspection
	for _, script := range []struct {
		name string
		data string
		dest *string
	}{
		{name: "KEYSREM", data: _RS_KEYSREM, dest: &br._KEYSREM},
		{name: "QDPOPTASKS", data: _RS_QDPOPTASKS, dest: &br._QDPOPTASKS},
		{name: "QDUPTASKS", data: _RS_QDUPTASKS, dest: &br._QDUPTASKS},
		{name: "QSTAT", data: _RS_QSTAT, dest: &br._QSTAT},
		{name: "QRPUSHTASK", data: _RS_QRPUSHTASK, dest: &br._QRPUSHTASK},
		{name: "QLPUSHTASK", data: _RS_QLPUSHTASK, dest: &br._QLPUSHTASK},
		{name: "QDPUSHTASK", data: _RS_QDPUSHTASK, dest: &br._QDPUSHTASK},
	} {
		switch sha1, legacyErr :=
			br.client.ScriptLoad(script.data).Result(); {

		case legacyErr != nil && legacyErr != redis.Nil:
			return nil, ekaerr.InitializationFailed.
				Wrap(legacyErr, s+"Failed to load Redus Lua script.").
				AddFields("bokchoy_redis_script", script.name).
				Throw()

		case sha1 == "":
			return nil, ekaerr.InitializationFailed.
				New(s+"Failed to load Redis Lua script. "+
					"Returned SHA1 is unexpectedly empty.").
				AddFields("bokchoy_redis_script", script.name).
				Throw()

		default:
			*script.dest = sha1
		}
	}

	return br, nil
}

// String returns a DSN using which Redis client of current RedisBroker
// is connected to some Redis server.
// Nil safe.
// Returns "<Incorrect Redis client">, if RedisBroker is not initialized properly.
func (p *RedisBroker) String() string {
	switch {
	case !p.isValid():
		return "<Incorrect Redis client>"
	}
	return p.clientInfo
}

// Ping pings the Redis client, that is set at the RedisBroker instantiation.
// Nil safe.
// Returns an error if RedisBroker is not initialized properly.
func (p *RedisBroker) Ping() *ekaerr.Error {
	const s = "Bokchoy.RedisBroker: Failed to ping Redis server. "

	switch {
	case !p.isValid():
		return ekaerr.IllegalState.
			New(s + "Broker is not initialized. " +
				"Did you even call NewBroker() to get that object?").
			Throw()
	}

	if legacyErr := p.client.Ping().Err(); legacyErr != nil {
		return ekaerr.ExternalError.
			Wrap(legacyErr, s).
			AddFields("bokchoy_broker", p.clientInfo).
			Throw()
	}

	return nil
}

// Consume fetches and returns all tasks from the current RedisBroker
// for the specified queue (by its name)
// which are ready for being consumed and processed right now
// in encoded RAW data format, depended of used bokchoy.Serializer.
//
// If an empty array of []byte is returned, there is no tasks.
//
// WARNING!
// Consider this is POP operation.
// Tasks returned by that method won't returned again by another that call,
// only if they aren't published again.
// If it's not what you need, take a look at the List() and Get() methods.
//
// WARNING!
// Does not return tasks which ETA is later than current time.
//
// Nil safe.
// Returns an error if RedisBroker is not initialized properly.
func (p *RedisBroker) Consume(queueName string, etaUnixNano int64) ([][]byte, *ekaerr.Error) {
	const s = "Bokchoy.RedisBroker: Failed to fetch tasks for being consumed. "

	var (
		result           []string
		queueKey         = buildKey1(queueName)
		queueKey2        = []string{queueKey}
		maxTTL           = time.Now().UnixNano()
		legacyErr        error
		usedRedisCommand string
		addLogMessage    string
	)

	if etaUnixNano == 0 {
		legacyErr = p.client.EvalSha(p._QDUPTASKS, queueKey2, maxTTL).Err()
		usedRedisCommand = "QDUPTASKS"
		addLogMessage = "Failed to try to up delayed tasks."
		if legacyErr == nil || legacyErr == redis.Nil {
			result, legacyErr = p.client.BLPop(p.tickInterval, queueKey).Result()
			usedRedisCommand = "BLPOP"
			addLogMessage = "Failed to retrieve ready-to-consumed tasks."
		}
	} else {
		var result_ interface{}
		result_, legacyErr = p.client.EvalSha(p._QDPOPTASKS, queueKey2, maxTTL).Result()
		usedRedisCommand = "QDPOPTASKS"
		addLogMessage = "Failed to retrieve delayed up-to tasks."
		if legacyErr == nil {
			result = result_.([]string)
		}
	}

	if etaUnixNano == 0 && len(result) > 0 {
		// Means BLPOP was used.
		// It always returns a queue (list name) as 1st return arg.
		// Ignore it.
		result = result[1:]
	}

	if legacyErr != nil && legacyErr != redis.Nil {
		return nil, ekaerr.ExternalError.
			Wrap(legacyErr, s+addLogMessage).
			AddFields(
				"bokchoy_queue_key", queueKey,
				"bokchoy_queue_name", queueName,
				"bokchoy_error_redis_command", usedRedisCommand).
			Throw()
	}

	if len(result) == 0 {
		return nil, nil
	}

	taskKeys := make([]string, 0, len(result))
	for _, taskID := range result {
		taskKeys = append(taskKeys, buildKey2(queueName, taskID))
	}

	var (
		encodedTasks [][]byte
		err          *ekaerr.Error
	)

	switch encodedTasks, err = p.getMany(taskKeys); {

	case err.IsNotNil():
		return nil, err.
			AddMessage(s).
			AddFields(
				"bokchoy_queue_key", queueKey,
				"bokchoy_queue_name", queueName).
			Throw()
	}

	return encodedTasks, nil
}

// Get returns a bokchoy.Task as its encoded RAW data from the current RedisBroker
// searching in the specified queue (by its name).
// It not removes task from its queue.
//
// Nil safe.
// Returns an error if RedisBroker is not initialized properly.
func (p *RedisBroker) Get(queueName, taskID string) ([]byte, *ekaerr.Error) {
	const s = "Bokchoy.RedisBroker: Failed to get specific task. "

	switch {
	case !p.isValid():
		return nil, ekaerr.IllegalState.
			New(s + "Broker is not initialized. " +
				"Did you even call NewBroker() to get that object?").
			Throw()
	}

	switch rawData, legacyErr :=
		p.client.Get(buildKey2(queueName, taskID)).Bytes(); {

	case legacyErr == redis.Nil:
		return nil, nil

	case legacyErr != nil:
		return nil, ekaerr.ExternalError.
			Wrap(legacyErr, s).
			AddFields(
				"bokchoy_queue_name", queueName,
				"bokchoy_task_id", taskID,
				"bokchoy_error_redis_command", "GET").
			Throw()

	default:
		return rawData, nil
	}
}

// Delete deletes a bokchoy.Task from its queue by bokchoy.Task's ID.
//
// WARNING!
// It not removes task from its queue, so queue will still contain task's metadata.
// That state will lead to one internal error you will get at the next Consume() call.
// It's not critical, but it's an error.
// Do it only if you absolutely understand why are you doing it.
//
// Nil safe.
// Returns an error if RedisBroker is not initialized properly.
//
// If requested task for being deleted is not exist, nil is returned,
// considering that it's not an error.
func (p *RedisBroker) Delete(queueName, taskID string) *ekaerr.Error {
	const s = "Bokchoy.RedisBroker: Failed to delete specific task. "

	switch {
	case !p.isValid():
		return ekaerr.IllegalState.
			New(s + "Broker is not initialized. " +
				"Did you even call NewBroker() to get that object?").
			Throw()
	}

	switch legacyErr :=
		p.client.Unlink(buildKey2(queueName, taskID)).Err(); {

	case legacyErr == redis.Nil:
		return nil

	case legacyErr != nil:
		return ekaerr.ExternalError.
			Wrap(legacyErr, s).
			AddFields(
				"bokchoy_queue_name", queueName,
				"bokchoy_task_id", taskID,
				"bokchoy_error_redis_command", "UNLINK").
			Throw()
	}

	return nil
}

// List fetches and returns all tasks from the current RedisBroker
// for the specified queue (by its name)
// which are ready for being consumed and processed right now
// in encoded RAW data format, depended of used bokchoy.Serializer.
//
// If an empty array of []byte is returned, there is no tasks.
//
// It looks like Consume(), but it not removes tasks from its queue.
// So, if you call Consume() then,
// you will get (at least) the same tasks as you got by the List().
//
// There is no way to "remove" task from queue totally.
// Delete() can do it, but not silently. It's rough but one way to do that.
// If you need to remove all tasks from queue, use Empty().
//
// WARNING!
// Does not return tasks which ETA is later than current time.
//
// Nil safe.
// Returns an error if RedisBroker is not initialized properly.
func (p *RedisBroker) List(queueName string) ([][]byte, *ekaerr.Error) {
	const s = "Bokchoy.RedisBroker: Failed to get tasks from queue. "

	switch {
	case !p.isValid():
		return nil, ekaerr.IllegalState.
			New(s + "Broker is not initialized. " +
				"Did you even call NewBroker() to get that object?").
			Throw()
	}

	var (
		taskIDs []string
	)

	switch taskIDs_, legacyErr :=
		p.client.LRange(queueName, 0, -1).Result(); {

	case legacyErr == redis.Nil:
		return nil, nil

	case legacyErr != nil:
		return nil, ekaerr.ExternalError.
			Wrap(legacyErr, s).
			AddFields(
				"bokchoy_queue_name", queueName,
				"bokchoy_error_redis_command", "LRANGE").
			Throw()

	default:
		taskIDs = taskIDs_
	}

	for i, n := 0, len(taskIDs); i < n; i++ {
		taskIDs[i] = buildKey2(queueName, taskIDs[i])
	}

	switch rawData, err :=
		p.getMany(taskIDs); {

	case err.IsNotNil():
		return nil, err.
			AddMessage(s).
			AddFields("bokchoy_queue_name", queueName).
			Throw()

	default:
		return rawData, nil
	}
}

// Count returns a stat about requested queue in the current RedisBroker.
// Using returned object you may figure out:
//
//  - Total number of tasks in the queue at this moment,
//  - Number of tasks that are delayed (ETA > current time),
//  - Number of tasks that are ready for being consumed (ETA <= current time).
//
// Nil safe.
// Returns an error if RedisBroker is not initialized properly.
func (p *RedisBroker) Count(queueName string) (bokchoy.BrokerStats, *ekaerr.Error) {
	const s = "Bokchoy.RedisBroker: Failed to get a queue stat. "

	switch {
	case !p.isValid():
		return bokchoy.BrokerStats{}, ekaerr.IllegalState.
			New(s + "Broker is not initialized. " +
				"Did you even call NewBroker() to get that object?").
			Throw()
	}

	var (
		stats bokchoy.BrokerStats
		err   *ekaerr.Error
	)

	switch res, legacyErr :=
		p.client.EvalSha(p._QSTAT, []string{buildKey1(queueName)}).Result(); {

	case legacyErr != nil:
		err = ekaerr.ExternalError.
			Wrap(legacyErr, s)

	default:
		// These errors below about bug.
		// They must NEVER (absolutely) happen, till you using valid Redis server,
		// and till Redis QSTAT Lua script is OK.
		//
		// If you catch any of these error, look at:
		//  - Redis server (including version),
		//  - go-redis ( https://github.com/go-redis/redis ) (including version),
		//  - Lua script.
		switch res, ok := res.([]int64); {

		case !ok:
			err = ekaerr.InternalError.
				New(s + "QSTAT returned value with not [2]int64 type. Bug?")

		case len(res) != 2:
			err = ekaerr.InternalError.
				New(s + "QSTAT returns []int64 with length != 2. Bug?")

		default:
			stats.Direct = int(res[0])
			stats.Delayed = int(res[1])
		}
	}

	//goland:noinspection GoNilness
	switch {

	case err.IsNotNil():
		return bokchoy.BrokerStats{}, err.
			AddFields(
				"bokchoy_queue_name", queueName,
				"bokchoy_error_redis_command", "QSTAT").
			Throw()
	}

	stats.Total = stats.Direct + stats.Delayed
	return stats, nil
}

// Set creates or updates existed bokchoy.Task in the current RedisBroker
// for the specified queue (by its name) with presented task's ID,
// its encoded RAW data and its TTL.
//
// Nil safe.
// Returns an error if RedisBroker is not initialized properly.
func (p *RedisBroker) Set(queueName, taskID string, data []byte, ttl time.Duration) *ekaerr.Error {
	const s = "Bokchoy.RedisBroker: Failed to create or update task. "

	switch {
	case !p.isValid():
		return ekaerr.IllegalState.
			New(s + "Broker is not initialized. " +
				"Did you even call NewBroker() to get that object?").
			Throw()
	}

	switch legacyErr :=
		p.client.Set(buildKey2(queueName, taskID), data, ttl).Err(); {

	case legacyErr != nil:
		return ekaerr.ExternalError.
			Wrap(legacyErr, s).
			AddFields(
				"bokchoy_queue_name", queueName,
				"bokchoy_task_id", taskID,
				"bokchoy_error_redis_command", "SET").
			Throw()
	}

	return nil
}

// Publish publishes bokchoy.Task as its task's ID, encoded RAW data and ETA.
//
// ETA determines HOW this task will be published:
//  - ETA == 0 means to publish task "as is" to the end of queue;
//  - ETA > current time means to publish task delayed to the specified part of queue,
//    where all delayed tasks are placed;
//  - ETA < current time means to publish task "as is" but w/ priority,
//    to the head of queue.
// ETA must be in unix time nanoseconds format.
//
// WARNING!
// You MUST NOT try to publish the same task twice, until it's not consumed
// (extracted) from the queue. Undefined behaviour otherwise.
//
// Nil safe.
// Returns an error if RedisBroker is not initialized properly.
func (p *RedisBroker) Publish(queueName, taskID string, data []byte, eta int64) *ekaerr.Error {
	const s = "Bokchoy.RedisBroker: Failed to publish task. "

	switch {
	case !p.isValid():
		return ekaerr.IllegalState.
			New(s + "Broker is not initialized. " +
				"Did you even call NewBroker() to get that object?").
			Throw()
	}

	var (
		legacyErr        error
		usedRedisCommand string
		queueKey         = []string{buildKey1(queueName)}
	)

	//goland:noinspection GoNilness
	switch now := time.Now().UnixNano(); {

	case eta == 0:
		legacyErr = p.client.EvalSha(p._QRPUSHTASK, queueKey, taskID, data).Err()
		usedRedisCommand = "QRPUSHTASK"

	case eta <= now:
		// ETA is before now, we should push this task in priority
		legacyErr = p.client.EvalSha(p._QLPUSHTASK, queueKey, taskID, data).Err()
		usedRedisCommand = "QLPUSHTASK"

	case eta > now:
		// ETA is after now, delay the task
		legacyErr = p.client.EvalSha(p._QDPUSHTASK, queueKey, taskID, eta, data).Err()
		usedRedisCommand = "QDPUSHTASK"
	}

	if legacyErr != nil {
		return ekaerr.ExternalError.
			Wrap(legacyErr, s).
			AddFields(
				"bokchoy_task_id", taskID,
				"bokchoy_queue_name", queueName,
				"bokchoy_error_redis_command", usedRedisCommand).
			Throw()
	}

	return nil
}

// Empty removes ABSOLUTELY ALL bokchoy.Task s from the current RedisBroker
// for the specified queue (by its name).
//
// WARNING!
// IT REMOVES NOT ONLY THE BODY OF TASKS BUT ALSO THE QUEUE'S METADATA KEYS,
// IN WHICH TASKS ORDERS OF EXECUTION ARE STORED.
// YOU CANNOT UNDONE THIS OPERATION!
//
// WARNING!
// Returns nil error if queue or/and tasks are not found.
//
// WARNING!
// Returns an instant error if queue's name is empty.
//
// Nil safe.
// Returns an error if RedisBroker is not initialized properly.
func (p *RedisBroker) Empty(queueName string) *ekaerr.Error {
	const s = "Bokchoy.RedisBroker: Failed to empty queue. "

	switch {
	case !p.isValid():
		return ekaerr.IllegalState.
			New(s + "Broker is not initialized. " +
				"Did you even call NewBroker() to get that object?").
			Throw()

	case queueName == "":
		// WE MUST AVOID TO PASS queueName == "",
		// CAUSE OTHERWISE ALL Bokchoy's RELATED DATA WILL BE DESTROYED!
		return ekaerr.IllegalArgument.
			New(s + "Empty queue name. " +
				"Use ClearAll() if you want to delete everything!").
			Throw()
	}

	var (
		legacyErr error
		queueKey  = []string{buildKey1(queueName + "*")}
	)

	switch legacyErr =
		p.client.EvalSha(p._KEYSREM, nil, queueKey).Err(); {

	case legacyErr != nil:
		return ekaerr.ExternalError.
			Wrap(legacyErr, s).
			AddFields(
				"bokchoy_queue_name", queueName,
				"bokchoy_error_redis_command", "KEYSREM").
			Throw()
	}

	return nil
}

// ClearAll removes ABSOLUTELY ALL bokchoy.Queue's data from the current RedisBroker,
// including its metadata, execution order, encoded tasks, etc. Everything.
//
// WARNING!
// ABSOLUTELY ALL BOKCHOY DATA WILL BE WIPED!
// YOU CANNOT UNDONE THIS OPERATION!
//
// WARNING!
// RedisBroker tries to do its best to remove ONLY its own data
// from the Redis DB, but there is a chance to remove your keys too.
// That collision may occur if the naming of your keys is the same as Bokchoy's ones.
//
// Nil safe.
// Returns an error if RedisBroker is not initialized properly.
func (p *RedisBroker) ClearAll() *ekaerr.Error {
	const s = "Bokchoy.RedisBroker: Failed to remove all. "

	switch {
	case !p.isValid():
		return ekaerr.IllegalState.
			New(s + "Broker is not initialized. " +
				"Did you even call NewBroker() to get that object?").
			Throw()
	}

	var (
		legacyErr error
		queueKey  = []string{buildKey1("*")}
	)

	switch legacyErr =
		p.client.EvalSha(p._KEYSREM, nil, queueKey).Err(); {

	case legacyErr != nil:
		return ekaerr.ExternalError.
			Wrap(legacyErr, s).
			AddFields(
				"bokchoy_error_redis_command", "KEYSREM").
			Throw()
	}

	return nil
}
