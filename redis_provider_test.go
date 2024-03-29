package hcache

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func redisProvider() Provider {
	return NewRedisCacheProvider(&RedisOptions{
		Prefix:      "cache",
		Pool:        cli(0),
		Marshaler:   MsgpackMarshal,
		Unmarshaler: MsgpackUnmarshal,
		DefaultTTL:  time.Minute,
	})
}

func TestRedisCacheProvider_Cache(t *testing.T) {
	p := redisProvider()
	abc := p.Cache("abc")
	def := p.Cache("def")

	assert.NotNil(t, abc)
	assert.NotNil(t, def)
	assert.NotEqual(t, abc, def)
}
