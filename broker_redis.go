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
	"sync"
	"time"

	"github.com/qioalice/ekago/v3/ekaerr"
	"github.com/qioalice/ekago/v3/ekalog"
	"github.com/qioalice/ekago/v3/ekaunsafe"

	"github.com/qioalice/bokchoy"

	"github.com/mediocregopher/radix/v4"
)

//goland:noinspection SpellCheckingInspection
type (
	// RedisBroker is the broker for Bokchoy that is used Redis as a backend.
	//
	// It uses Redis' zset to store metadata of tasks (an order of tasks per queue),
	// where rank is unix timestamp of data when task must be processed.
	// Also Redis' key-value pair is used, when key is a queue name + task's ID (ULID)
	// and value is msgpack'ed task's payload.
	// A user's payload can be encoded any way user wants. Typically it's JSON.
	//
	// WARNING!
	// YOUR REDIS SERVER'S VERSION MUST BE 6.2.0 OR GREATER!
	RedisBroker struct {

		// --- Main parts ---

		client radix.Client
		clientInfo string

		// --- Additional parts ---

		logger *ekalog.Logger

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

//goland:noinspection GoSnakeCaseUsage
const (
	// CTX_TIMEOUT_DEFAULT is a timeout that is provided for Redis command to be executed.
	CTX_TIMEOUT_DEFAULT = 5 * time.Second
)

// NewBroker creates, initializes and returns a new RedisBroker instance.
// It uses a passed Option s to determine its behaviour.
//
// There are required options, you have to specify:
//
//  - WithRedisClient():
//    You need to specify Redis client, what will be used as backend.
//    Keep in mind, your Redis server's version must be 6.2.0 or greater.
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
		client:          optionsObject.Client,
		logger:          ekalog.Copy(),
		tickInterval:    time.Second,
		qcd:             make(map[string]struct{}),
		mu:              &sync.Mutex{},
	}

	if optionsObject.TickInterval > time.Second {
		br.tickInterval = optionsObject.TickInterval
	}

	if optionsObject.Logger != nil {
		br.logger = optionsObject.Logger
	}
	
	// Get an info about Redis' server and current client.
	// First of call, check Redis version using INFO command.

	kvSplit := func(s string, sep byte) (key, value string) {
		idx := strings.IndexByte(s, sep)
		if idx == -1 {
			idx = 0
		}
		return s[:idx], s[idx+1:]
	}

	var respStrRaw string

	// --- Redis server ver 6.2.0 or greater CHECK --- //

	if _, err = br.execCmd("INFO", &respStrRaw, CTX_TIMEOUT_DEFAULT, true); err.IsNotNil() {
		return nil, err.ReplaceClass(ekaerr.InitializationFailed).AddMessage(s).Throw()
	}

	respLines := strings.Split(respStrRaw, "\n")
	if len(respLines) < 2 {
		return nil, ekaerr.InitializationFailed.
			New(s + "INFO command returns strange response. Do you use Redis <= 2.4 version?").
			Throw()
	}

	redisVersionKey, redisVersion := kvSplit(respLines[1], ':')
	redisVersion = strings.TrimSpace(redisVersion)
	if redisVersionKey != "redis_version" || redisVersion == "" {
		return nil, ekaerr.InitializationFailed.
			New(s + "INFO command contains strange 2nd line of Server information. Do you use Redis <= 2.4 version?").
			WithString("bokchoy_redis_server_version", respLines[1]).
			Throw()
	}

	redisVersionParts := strings.Split(redisVersion, ".")
	rvMajor := redisVersionParts[0] // checks above guarantees that at least 1 elem will be presented
	rvMinor := "0"

	if len(redisVersionParts) > 1 {
		rvMinor = redisVersionParts[1]
	}

	rvMajorInt, legacyErr := strconv.Atoi(rvMajor)
	if legacyErr != nil {
		return nil, ekaerr.InitializationFailed.
			Wrap(legacyErr, s + "After INFO command failed to parse Server major version.").
			WithString("bokchoy_redis_server_major_version", rvMajor).
			Throw()
	}
	rvMinorInt, legacyErr := strconv.Atoi(rvMinor)
	if legacyErr != nil {
		return nil, ekaerr.InitializationFailed.
			Wrap(legacyErr, s + "After INFO command failed to parse Server minor version.").
			WithString("bokchoy_redis_server_minor_version", rvMinor).
			Throw()
	}

	if rvMajorInt < 6 || (rvMajorInt == 6 && rvMinorInt < 2) {
		return nil, ekaerr.InitializationFailed.
			New(s + "Redis server must be version 6.2.0 or greater.").
			WithString("bokchoy_redis_server_version", redisVersion).
			Throw()
	}

	// --- Redis server ver 6.2.0 or greater CONFIRMED --- //

	br.clientInfo = "Redis " + redisVersion

	// --- Redis server and client data REQUESTING --- //

	if _, err = br.execCmd("CLIENT", &respStrRaw, CTX_TIMEOUT_DEFAULT, true, "INFO"); err.IsNil() {
		var (
			db = "?"
			addr = "?"
		)
		for _, part := range strings.Split(respStrRaw, " ") {
			switch k, v := kvSplit(part, '='); {
			case k == "db" && v != "": db = v
			case k == "addr" && v != "" && addr == "?": addr = v
			case k == "laddr" && v != "": addr = v
			}
		}
		br.clientInfo += ": " + addr + "/" + db
	}

	// --- Redis server and client data RETRIEVED. --- //

	// --- Redis registering custom Lua scripts START. --- //

	//goland:noinspection SpellCheckingInspection
	for _, script := range []struct {
		name string
		data string
		dest *string
	}{
		{ name: "KEYSREM",    data: _RS_KEYSREM,    dest: &br._KEYSREM    },
		{ name: "QDPOPTASKS", data: _RS_QDPOPTASKS, dest: &br._QDPOPTASKS },
		{ name: "QDUPTASKS",  data: _RS_QDUPTASKS,  dest: &br._QDUPTASKS  },
		{ name: "QSTAT",      data: _RS_QSTAT,      dest: &br._QSTAT      },
		{ name: "QRPUSHTASK", data: _RS_QRPUSHTASK, dest: &br._QRPUSHTASK },
		{ name: "QLPUSHTASK", data: _RS_QLPUSHTASK, dest: &br._QLPUSHTASK },
		{ name: "QDPUSHTASK", data: _RS_QDPUSHTASK, dest: &br._QDPUSHTASK },
	} {
		if _, err = br.execCmd("SCRIPT", &respStrRaw, CTX_TIMEOUT_DEFAULT, true, "LOAD", script.data); err.IsNotNil() {
			return nil, err.ReplaceClass(ekaerr.InitializationFailed).
				AddMessage(s).
				WithString("bokchoy_redis_server_version", redisVersion).
				WithString("bokchoy_redis_script_name", script.name).
				Throw()
		}
		*script.dest = respStrRaw
	}

	// --- Redis registering custom Lua scripts COMPLETED. --- //

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

	if _, err := p.execCmd("PING", nil, CTX_TIMEOUT_DEFAULT, false); err.IsNotNil() {
		return err.AddMessage(s).WithString("bokchoy_broker", p.clientInfo).Throw()
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
		maxTTL           = time.Now().UnixNano()
		err              *ekaerr.Error
		encodedTasks     [][]byte
		addLogMessage    string
	)

	if etaUnixNano == 0 {
		if _, err = p.execFlatCmd("EVALSHA", nil, CTX_TIMEOUT_DEFAULT, false, p._QDUPTASKS, "1", queueKey, maxTTL); err.IsNotNil() {
			addLogMessage = "Failed to try to up delayed tasks."

		} else if _, err = p.execFlatCmd("BLPOP", &result, p.tickInterval + CTX_TIMEOUT_DEFAULT, false, queueKey, p.tickInterval.Seconds()); err.IsNotNil() {
			addLogMessage = "Failed to retrieve ready-to-consumed tasks."
		}
	} else {
		if _, err = p.execFlatCmd("EVALSHA", &result, CTX_TIMEOUT_DEFAULT, false, p._QDPOPTASKS, "1", queueKey, maxTTL); err.IsNotNil() {
			addLogMessage = "Failed to retrieve delayed up-to tasks."
		}
	}

	if etaUnixNano == 0 && len(result) > 0 {
		// Means BLPOP was used.
		// It always returns a queue (list name) as 1st return arg. Ignore it.
		result = result[1:]
	}

	if err.IsNil() {
		if len(result) == 0 {
			return nil, nil
		}

		taskKeys := make([]string, 0, len(result))
		for _, taskID := range result {
			taskKeys = append(taskKeys, buildKey2(queueName, taskID))
		}

		encodedTasks, err = p.getMany(taskKeys)
	}

	if err.IsNotNil() {
		return nil, err.AddMessage(s + addLogMessage).
			WithString("bokchoy_queue_key", queueKey).
			WithString("bokchoy_queue_name", queueName).
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

	var resp []byte
	key := buildKey2(queueName, taskID)
	if _, err := p.execCmd("GET", &resp, CTX_TIMEOUT_DEFAULT, false, key); err.IsNotNil() {
		return nil, err.
			AddMessage(s).
			WithString("bokchoy_queue_name", queueName).
			WithString("bokchoy_task_id", taskID).
			Throw()
	}

	return resp, nil
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

	if _, err := p.execCmd("UNLINK", nil, CTX_TIMEOUT_DEFAULT, false); err.IsNotNil() {
		return err.
			AddMessage(s).
			WithString("bokchoy_queue_name", queueName).
			WithString("bokchoy_task_id", taskID).
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

	var taskIDs []string
	if _, err := p.execCmd("LRANGE", &taskIDs, CTX_TIMEOUT_DEFAULT, false, "0", "-1"); err.IsNotNil() {
		return nil, err.AddMessage(s).WithString("bokchoy_queue_name", queueName).Throw()
	}

	for i, n := 0, len(taskIDs); i < n; i++ {
		taskIDs[i] = buildKey2(queueName, taskIDs[i])
	}

	if rawData, err := p.getMany(taskIDs); err.IsNotNil() {
		return nil, err.AddMessage(s).WithString("bokchoy_queue_name", queueName).Throw()
	} else {
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
		rawResp []string
		stats bokchoy.BrokerStats
	)

	key := buildKey1(queueName)
	if _, err := p.execCmd("EVALSHA", &rawResp, CTX_TIMEOUT_DEFAULT, true, p._QSTAT, key); err.IsNotNil() {
		return stats, err.AddMessage(s).WithString("bokchoy_queue_name", queueName).Throw()
	}

	// These errors below about bug.
	// They must NEVER (absolutely) happen, till you using valid Redis server,
	// and till Redis QSTAT Lua script is OK.
	//
	// If you catch any of these error, look at:
	//  - Redis server (including version),
	//  - radix ( https://github.com/mediocregopher/radix ) (including version),
	//  - Lua script.

	if len(rawResp) != 2 {
		return stats, ekaerr.InternalError.
			New(s + "QSTAT returns response with length != 2. Bug?").
			WithString("bokchoy_queue_name", queueName).
			Throw()
	}

	var legacyErr error
	if stats.Direct, legacyErr = strconv.Atoi(rawResp[0]); legacyErr != nil {
		return stats, ekaerr.InternalError.
			New(s + "QSTAT returns response and 1st argument is not integer. Bug?").
			WithString("bokchoy_queue_name", queueName).
			Throw()
	}
	if stats.Delayed, legacyErr = strconv.Atoi(rawResp[1]); legacyErr != nil {
		return stats, ekaerr.InternalError.
			New(s + "QSTAT returns response and 2nd argument is not integer. Bug?").
			WithString("bokchoy_queue_name", queueName).
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

	var err *ekaerr.Error
	key := buildKey2(queueName, taskID)

	if ttl > time.Second {
		_, err = p.execFlatCmd("SETEX", nil, CTX_TIMEOUT_DEFAULT, true, key, ttl.Seconds(), data)
	} else {
		_, err = p.execFlatCmd("SET", nil, CTX_TIMEOUT_DEFAULT, true, key, data)
	}

	if err.IsNotNil() {
		return err.
			AddMessage(s).
			WithString("bokchoy_queue_name", queueName).
			WithString("bokchoy_task_id", taskID).
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

	var err *ekaerr.Error
	key := buildKey1(queueName)

	switch now := time.Now().UnixNano(); {

	case eta == 0:
		_, err = p.execFlatCmd("EVALSHA", nil, CTX_TIMEOUT_DEFAULT, true, p._QRPUSHTASK, "1", key, taskID, data)

	case eta <= now:
		_, err = p.execFlatCmd("EVALSHA", nil, CTX_TIMEOUT_DEFAULT, true, p._QLPUSHTASK, "1", key, taskID, data)

	case eta > now:
		// ETA is after now, delay the task
		_, err = p.execFlatCmd("EVALSHA", nil, CTX_TIMEOUT_DEFAULT, true, p._QDPUSHTASK, "1", key, taskID, eta, data)
	}

	//goland:noinspection GoNilness
	if err.IsNotNil() {
		return err.
			AddMessage(s).
			WithString("bokchoy_queue_name", queueName).
			WithString("bokchoy_task_id", taskID).
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

	key := buildKey1(queueName + "*")
	if _, err := p.execCmd("EVALSHA", nil, CTX_TIMEOUT_DEFAULT, true, p._KEYSREM, "1", key); err.IsNotNil() {
		return err.AddMessage(s).WithString("bokchoy_queue_name", queueName).Throw()
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

	key := buildKey1("*")
	if _, err := p.execCmd("EVALSHA", nil, CTX_TIMEOUT_DEFAULT, true, p._KEYSREM, "1", key); err.IsNotNil() {
		return err.AddMessage(s).Throw()
	}

	return nil
}
