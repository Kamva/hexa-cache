package hcache

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func redisProvider() Provider {
	return NewRedisCacheProvider(&RedisOptions{
		Prefix:      "cache_",
		Client:      cli(0),
		Marshaler:   MsgpackMarshaler,
		Unmarshaler: MsgpackUnmarshaler,
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
