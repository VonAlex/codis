package zk

import (
	"errors"
)

const (
	protocolVersion = 0

	DefaultPort = 2181
)

const (
	opNotify       = 0
	opCreate       = 1
	opDelete       = 2
	opExists       = 3
	opGetData      = 4
	opSetData      = 5
	opGetAcl       = 6
	opSetAcl       = 7
	opGetChildren  = 8
	opSync         = 9
	opPing         = 11
	opGetChildren2 = 12
	opCheck        = 13
	opMulti        = 14
	opClose        = -11
	opSetAuth      = 100
	opSetWatches   = 101
	// Not in protocol, used internally
	opWatcherEvent = -2
)

const (
	EventNodeCreated         = EventType(1)
	EventNodeDeleted         = EventType(2)
	EventNodeDataChanged     = EventType(3)
	EventNodeChildrenChanged = EventType(4)

	EventSession     = EventType(-1)
	EventNotWatching = EventType(-2)
)

var (
	eventNames = map[EventType]string{
		EventNodeCreated:         "EventNodeCreated",
		EventNodeDeleted:         "EventNodeDeleted",
		EventNodeDataChanged:     "EventNodeDataChanged",
		EventNodeChildrenChanged: "EventNodeChildrenChanged",
		EventSession:             "EventSession",
		EventNotWatching:         "EventNotWatching",
	}
)

const (
	StateUnknown           = State(-1)
	StateDisconnected      = State(0)
	StateConnecting        = State(1)
	StateSyncConnected     = State(3)
	StateAuthFailed        = State(4)
	StateConnectedReadOnly = State(5)
	StateSaslAuthenticated = State(6)
	// 在和 zk server 成功建立 TCP 连接之后，握手阶段发现 session 超时
	StateExpired = State(-112)
	// StateAuthFailed        = State(-113)

	// 在和 zk server 成功建立 TCP 连接之后的状态
	StateConnected = State(100)
	// 和zk server 成功建立 TCP 连接，并且成功握手(即成功创建 session)
	StateHasSession = State(101)
)

const (
	// 0: 永久节点，除非手动删除
	// 创建临时节点，session 断开则节点也被删除
	FlagEphemeral = 1
	// 持久时序节点，会自动在节点后面添加序号
	FlagSequence = 2
	// 3: Ephemeral 和 Sequence，即，短暂且自动添加序号
)

var (
	stateNames = map[State]string{
		StateUnknown:           "StateUnknown",
		StateDisconnected:      "StateDisconnected",
		StateSyncConnected:     "StateSyncConnected",
		StateConnectedReadOnly: "StateConnectedReadOnly",
		StateSaslAuthenticated: "StateSaslAuthenticated",
		StateExpired:           "StateExpired",
		StateAuthFailed:        "StateAuthFailed",
		StateConnecting:        "StateConnecting",
		StateConnected:         "StateConnected",
		StateHasSession:        "StateHasSession",
	}
)

type State int32

func (s State) String() string {
	if name := stateNames[s]; name != "" {
		return name
	}
	return "Unknown"
}

type ErrCode int32

var (
	ErrConnectionClosed        = errors.New("zk: connection closed")
	ErrUnknown                 = errors.New("zk: unknown error")
	ErrAPIError                = errors.New("zk: api error")
	ErrNoNode                  = errors.New("zk: node does not exist")
	ErrNoAuth                  = errors.New("zk: not authenticated")
	ErrBadVersion              = errors.New("zk: version conflict")
	ErrNoChildrenForEphemerals = errors.New("zk: ephemeral nodes may not have children")
	ErrNodeExists              = errors.New("zk: node already exists")
	ErrNotEmpty                = errors.New("zk: node has children")
	ErrSessionExpired          = errors.New("zk: session has been expired by the server")
	ErrInvalidACL              = errors.New("zk: invalid ACL specified")
	ErrAuthFailed              = errors.New("zk: client authentication failed")
	ErrClosing                 = errors.New("zk: zookeeper is closing")
	ErrNothing                 = errors.New("zk: no server responsees to process")
	ErrSessionMoved            = errors.New("zk: session moved to another server, so operation is ignored")

	// ErrInvalidCallback         = errors.New("zk: invalid callback specified")
	errCodeToError = map[ErrCode]error{
		0:                          nil,
		errAPIError:                ErrAPIError,
		errNoNode:                  ErrNoNode,
		errNoAuth:                  ErrNoAuth,
		errBadVersion:              ErrBadVersion,
		errNoChildrenForEphemerals: ErrNoChildrenForEphemerals,
		errNodeExists:              ErrNodeExists,
		errNotEmpty:                ErrNotEmpty,
		errSessionExpired:          ErrSessionExpired,
		// errInvalidCallback:         ErrInvalidCallback,
		errInvalidAcl:   ErrInvalidACL,
		errAuthFailed:   ErrAuthFailed,
		errClosing:      ErrClosing,
		errNothing:      ErrNothing,
		errSessionMoved: ErrSessionMoved,
	}
)

func (e ErrCode) toError() error {
	if err, ok := errCodeToError[e]; ok {
		return err
	}
	return ErrUnknown
}

const (
	errOk = 0
	// System and server-side errors
	errSystemError          = -1
	errRuntimeInconsistency = -2
	errDataInconsistency    = -3
	errConnectionLoss       = -4
	errMarshallingError     = -5
	errUnimplemented        = -6
	errOperationTimeout     = -7
	errBadArguments         = -8
	errInvalidState         = -9
	// API errors
	errAPIError                = ErrCode(-100)
	errNoNode                  = ErrCode(-101) // *
	errNoAuth                  = ErrCode(-102)
	errBadVersion              = ErrCode(-103) // *
	errNoChildrenForEphemerals = ErrCode(-108)
	errNodeExists              = ErrCode(-110) // *
	errNotEmpty                = ErrCode(-111)
	errSessionExpired          = ErrCode(-112)
	errInvalidCallback         = ErrCode(-113)
	errInvalidAcl              = ErrCode(-114)
	errAuthFailed              = ErrCode(-115)
	errClosing                 = ErrCode(-116)
	errNothing                 = ErrCode(-117)
	errSessionMoved            = ErrCode(-118)
)

// Constants for ACL permissions
const (
	PermRead = 1 << iota
	PermWrite
	PermCreate
	PermDelete
	PermAdmin
	PermAll = 0x1f
)

var (
	emptyPassword = []byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}
	opNames       = map[int32]string{
		opNotify:       "notify",
		opCreate:       "create",
		opDelete:       "delete",
		opExists:       "exists",
		opGetData:      "getData",
		opSetData:      "setData",
		opGetAcl:       "getACL",
		opSetAcl:       "setACL",
		opGetChildren:  "getChildren",
		opSync:         "sync",
		opPing:         "ping",
		opGetChildren2: "getChildren2",
		opCheck:        "check",
		opMulti:        "multi",
		opClose:        "close",
		opSetAuth:      "setAuth",
		opSetWatches:   "setWatches",

		opWatcherEvent: "watcherEvent",
	}
)

type EventType int32

func (t EventType) String() string {
	if name := eventNames[t]; name != "" {
		return name
	}
	return "Unknown"
}
