package hcache

import (
	"context"
	"testing"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func cli(dbNum int) *redis.Pool {
	return &redis.Pool{
		Dial: func() (redis.Conn, error) {
			return redis.Dial("tcp", "localhost:6379")
		},
		MaxIdle:     3,
		IdleTimeout: 240 * time.Second,
	}
}

func flushRedisDb(cli *redis.Pool) error {
	_, err := cli.Get().Do("FLUSHDB")
	return err
}

func TestNewRedisCache(t *testing.T) {
	r := NewRedisCache("abc", &RedisOptions{
		Prefix:      "cache_",
		Pool:        cli(0),
		Marshaler:   MsgpackMarshal,
		Unmarshaler: MsgpackUnmarshal,
		DefaultTTL:  time.Second * 2,
	}).(*redisCache)

	if assert.NotNil(t, r) {
		return
	}

	assert.Equal(t, "abc", r.name)
	assert.Equal(t, "cache_", r.prefix)
	assert.Equal(t, time.Second*2, r.defaultTTL)
	assert.Equal(t, MsgpackMarshal, r.marshal)
	assert.Equal(t, MsgpackUnmarshal, r.unmarshal)
}

func TestRedisCache_key(t *testing.T) {
	cases := []struct {
		Tag       string
		Key       string
		ResultKey string
	}{
		{"t1", "", "cache_abc_"},
		{"t2", "123", "cache_abc_123"},
		{"t3", "*", "cache_abc_*"},
	}

	r := NewRedisCache("abc", &RedisOptions{
		Prefix:      "cache_",
		Pool:        cli(0),
		Marshaler:   MsgpackMarshal,
		Unmarshaler: MsgpackUnmarshal,
		DefaultTTL:  time.Second * 2,
	}).(*redisCache)

	for _, c := range cases {
		t.Run(c.Tag, func(t *testing.T) {
			assert.Equal(t, c.ResultKey, r.key(c.Key))
		})
	}
}

func TestRedisCache_Name(t *testing.T) {
	r := NewRedisCache("abc", &RedisOptions{
		Prefix:      "cache_",
		Pool:        cli(0),
		Marshaler:   MsgpackMarshal,
		Unmarshaler: MsgpackUnmarshal,
		DefaultTTL:  time.Second * 2,
	})

	assert.Equal(t, "abc", r.Name())
}

func TestRedisCache_SetWithTTL(t *testing.T) {
	client := cli(0)
	r := NewRedisCache("abc", &RedisOptions{
		Prefix:      "cache_",
		Pool:        client,
		Marshaler:   MsgpackMarshal,
		Unmarshaler: MsgpackUnmarshal,
		DefaultTTL:  time.Second * 2,
	})
	marshaledHi := "\xa2hi" // [162,104,105]

	cases := []struct {
		Tag          string
		Key          string
		FullKey      string
		Val          string
		MarshaledVal string
		TTL          time.Duration
	}{
		{"t1", "k1", "cache_abc_k1", "hi", marshaledHi, 0}, // forever
		{"t2", "k2", "cache_abc_k2", "hi", marshaledHi, 0}, // forever
		{"t3", "k3", "cache_abc_k3", "hi", marshaledHi, 0}, // forever
		{"t3", "k4", "cache_abc_k4", "hi", marshaledHi, time.Millisecond * 2},
	}

	ctx := context.Background()
	for _, c := range cases {
		var val string
		t.Run(c.Tag, func(t *testing.T) {
			// Just to make sure full-key is true:
			assert.Equal(t, c.FullKey, r.(*redisCache).key(c.Key))

			if !assert.Nil(t, r.SetWithTTL(ctx, c.Key, c.Val, c.TTL)) {
				return
			}

			realVal, err := redis.String(client.Get().Do("GET", c.FullKey))
			assert.Nil(t, err)
			assert.Equal(t, c.MarshaledVal, realVal)

			if c.TTL != 0 {
				time.Sleep(c.TTL)

				realVal, err := redis.String(client.Get().Do("GET", c.FullKey))

				// Should be expired and return empty value for it.
				assert.Equal(t, "", realVal)
				assert.Equal(t, redis.ErrNil, err)
				assert.Equal(t, ErrKeyNotFound, r.Get(ctx, c.Key, nil), &val)
			}
		})
	}

}

func TestRedisCache_Get(t *testing.T) {
	client := cli(0)
	r := NewRedisCache("abc", &RedisOptions{
		Prefix:      "cache_",
		Pool:        client,
		Marshaler:   MsgpackMarshal,
		Unmarshaler: MsgpackUnmarshal,
		DefaultTTL:  time.Second * 2,
	})
	marshaledHi := "\xa2hi" // [162,104,105]

	cases := []struct {
		Tag          string
		Key          string
		FullKey      string
		Val          string
		MarshaledVal string
		TTL          time.Duration
	}{
		{"t1", "k1", "cache_abc_k1", "hi", marshaledHi, 0}, // forever
		{"t2", "k2", "cache_abc_k2", "hi", marshaledHi, 0}, // forever
		{"t3", "k3", "cache_abc_k3", "hi", marshaledHi, 0}, // forever
		{"t3", "k4", "cache_abc_k4", "hi", marshaledHi, time.Millisecond * 2},
	}

	ctx := context.Background()
	for _, c := range cases {
		t.Run(c.Tag, func(t *testing.T) {
			// Just to make sure full-key is true:
			assert.Equal(t, c.FullKey, r.(*redisCache).key(c.Key))

			// Set the value
			if c.TTL != 0 {
				_, err := client.Get().Do("SET", c.FullKey, c.MarshaledVal, "PX", c.TTL.Milliseconds())
				assert.Nil(t, err)
			} else {
				_, err := client.Get().Do("SET", c.FullKey, c.MarshaledVal)
				assert.Nil(t, err)
			}

			var val string
			require.Nil(t, r.Get(ctx, c.Key, &val))
			assert.Equal(t, c.Val, val)

			if c.TTL != 0 {
				time.Sleep(c.TTL)
				// Should be expired and return empty value for it.
				assert.Equal(t, ErrKeyNotFound, r.Get(ctx, c.Key, &val))
			}
		})
	}
}

func TestRedisCache_Remove(t *testing.T) {
	client := cli(0)
	r := NewRedisCache("abc", &RedisOptions{
		Prefix:      "cache_",
		Pool:        client,
		Marshaler:   MsgpackMarshal,
		Unmarshaler: MsgpackUnmarshal,
		DefaultTTL:  time.Second * 2,
	})
	var val string

	ctx := context.Background()
	require.Nil(t, r.Set(ctx, "k1", "hi"))
	require.Nil(t, r.Set(ctx, "k2", "hi"))

	require.Nil(t, r.Remove(ctx, "k1"))
	assert.Equal(t, ErrKeyNotFound, r.Get(ctx, "k1", &val))

	// We should have the k2
	require.Nil(t, r.Get(ctx, "k2", &val))
	assert.Equal(t, "hi", val)

	// Delete the k2
	require.Nil(t, r.Remove(ctx, "k2"))
	assert.Equal(t, ErrKeyNotFound, r.Get(ctx, "k2", &val))
}

func TestRedisCache_Purge(t *testing.T) {
	client := cli(0)
	r := NewRedisCache("abc", &RedisOptions{
		Prefix:      "cache_",
		Pool:        client,
		Marshaler:   MsgpackMarshal,
		Unmarshaler: MsgpackUnmarshal,
		DefaultTTL:  time.Second * 2,
	})
	var val string

	ctx := context.Background()

	// Set another value which is not for the cache to make sure it doesn't remove any value!
	_, err := client.Get().Do("SET", "another_key", "val")
	require.Nil(t, err)

	require.Nil(t, r.Set(ctx, "k1", "hi"))
	require.Nil(t, r.Set(ctx, "k2", "hi"))

	require.Nil(t, r.Purge(ctx))

	assert.Equal(t, ErrKeyNotFound, r.Get(ctx, "k1", &val))
	assert.Equal(t, ErrKeyNotFound, r.Get(ctx, "k2", &val))
	res, err := redis.String(client.Get().Do("GET", "another_key"))
	assert.Nil(t, err)
	assert.Equal(t, "val", res)
}
