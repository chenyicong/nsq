package nsqd

// the core algorithm here was borrowed from:
// Blake Mizerany's `noeqd` https://github.com/bmizerany/noeqd
// and indirectly:
// Twitter's `snowflake` https://github.com/twitter/snowflake

// only minor cleanup and changes to introduce a type, combine the concept
// of workerID + datacenterId into a single identifier, and modify the
// behavior when sequences rollover for our specific implementation needs

import (
	"encoding/hex"
	"errors"
	"sync"
	"time"
)

const (
	nodeIDBits     = uint64(10)
	sequenceBits   = uint64(12)
	nodeIDShift    = sequenceBits
	timestampShift = sequenceBits + nodeIDBits
	sequenceMask   = int64(-1) ^ (int64(-1) << sequenceBits)

	// ( 2012-10-28 16:23:42 UTC ).UnixNano() >> 20
	twepoch = int64(1288834974288)
)

var ErrTimeBackwards = errors.New("time has gone backwards")
var ErrSequenceExpired = errors.New("sequence expired")
var ErrIDBackwards = errors.New("ID went backward")

type guid int64

type guidFactory struct {
	sync.Mutex

	nodeID        int64
	sequence      int64
	lastTimestamp int64
	lastID        guid
}

func NewGUIDFactory(nodeID int64) *guidFactory {
	return &guidFactory{
		nodeID: nodeID,
	}
}

/*
	最高1bit不用（二进制中最高位为1的都是负数，但是我们生成的id一般都使用整数，所以这个最高位固定是0），
	默认情况下41bit的时间戳可以支持该算法使用到2082年，
	10bit的工作机器id可以支持1023台机器，
	序列号支持1毫秒产生4095个自增序列id
*/
func (f *guidFactory) NewGUID() (guid, error) {
	f.Lock()

	// divide by 1048576, giving pseudo-milliseconds
	// 1 ts = nano >> 20，而1 milliseconds = 1000000 nano，所以注释里说milliseconds
	ts := time.Now().UnixNano() >> 20

	if ts < f.lastTimestamp {
		f.Unlock()
		return 0, ErrTimeBackwards
	}

	if f.lastTimestamp == ts {
		f.sequence = (f.sequence + 1) & sequenceMask
		if f.sequence == 0 {
			f.Unlock()
			return 0, ErrSequenceExpired
		}
	} else {
		f.sequence = 0
	}

	f.lastTimestamp = ts

	id := guid(((ts - twepoch) << timestampShift) |
		(f.nodeID << nodeIDShift) |
		f.sequence)

	if id <= f.lastID {
		f.Unlock()
		return 0, ErrIDBackwards
	}

	f.lastID = id

	f.Unlock()

	return id, nil
}

func (g guid) Hex() MessageID {
	var h MessageID
	var b [8]byte

	b[0] = byte(g >> 56)
	b[1] = byte(g >> 48)
	b[2] = byte(g >> 40)
	b[3] = byte(g >> 32)
	b[4] = byte(g >> 24)
	b[5] = byte(g >> 16)
	b[6] = byte(g >> 8)
	b[7] = byte(g)

	hex.Encode(h[:], b[:])
	return h
}
