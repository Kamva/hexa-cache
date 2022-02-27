package hcache

import (
	"github.com/kamva/tracer"
	"github.com/tinylib/msgp/msgp"
	"github.com/vmihailenco/msgpack/v5"
)

type Marshaler func(val interface{}) ([]byte, error)

type Unmarshaler func(msg []byte, val interface{}) error

func MsgpackMarshaler(val interface{}) ([]byte, error) {
	// The fast path (using generated code)
	if msgpVal, ok := val.(msgp.Marshaler); ok {
		return msgpVal.MarshalMsg(nil)
	}

	// The slow path
	return msgpack.Marshal(val)
}

func MsgpackUnmarshaler(msg []byte, val interface{}) error {
	// The fast path (using generated code)
	if msgpVal, ok := val.(msgp.Unmarshaler); ok {
		_, err := msgpVal.UnmarshalMsg(msg)
		return tracer.Trace(err)
	}

	// The slow path
	return msgpack.Unmarshal(msg, &val)
}
