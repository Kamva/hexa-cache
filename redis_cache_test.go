package hcache

import (
	"context"
	"testing"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func cli(dbNum int) *redis.Client {
	return redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "",    // no password set
		DB:       dbNum, // use default DB
	})
}

func flushRedisDb(cli *redis.Client) error {
	return cli.FlushDB(context.Background()).Err()
}

func TestNewRedisCache(t *testing.T) {
	r := NewRedisCache("abc", &RedisOptions{
		Prefix:      "cache_",
		Client:      cli(0),
		Marshaler:   MsgpackMarshaler,
		Unmarshaler: MsgpackUnmarshaler,
		DefaultTTL:  time.Second * 2,
	}).(*redisCache)

	if assert.NotNil(t, r) {
		return
	}

	assert.Equal(t, "abc", r.name)
	assert.Equal(t, "cache_", r.prefix)
	assert.Equal(t, time.Second*2, r.defaultTTL)
	assert.Equal(t, MsgpackMarshaler, r.marshal)
	assert.Equal(t, MsgpackUnmarshaler, r.unmarshal)
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
		Client:      cli(0),
		Marshaler:   MsgpackMarshaler,
		Unmarshaler: MsgpackUnmarshaler,
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
		Client:      cli(0),
		Marshaler:   MsgpackMarshaler,
		Unmarshaler: MsgpackUnmarshaler,
		DefaultTTL:  time.Second * 2,
	})

	assert.Equal(t, "abc", r.Name())
}

func TestRedisCache_SetWithTTL(t *testing.T) {
	client := cli(0)
	r := NewRedisCache("abc", &RedisOptions{
		Prefix:      "cache_",
		Client:      client,
		Marshaler:   MsgpackMarshaler,
		Unmarshaler: MsgpackUnmarshaler,
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

			assert.Equal(t, c.MarshaledVal, client.Get(ctx, c.FullKey).Val())

			if c.TTL != 0 {
				time.Sleep(c.TTL)
				// Should be expired and return empty value for it.
				assert.Equal(t, "", client.Get(ctx, c.FullKey).Val())
				assert.Equal(t, ErrKeyNotFound, r.Get(ctx, c.Key, nil), &val)
			}
		})
	}

}

func TestRedisCache_Get(t *testing.T) {
	client := cli(0)
	r := NewRedisCache("abc", &RedisOptions{
		Prefix:      "cache_",
		Client:      client,
		Marshaler:   MsgpackMarshaler,
		Unmarshaler: MsgpackUnmarshaler,
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
			require.Nil(t, client.Set(ctx, c.FullKey, c.MarshaledVal, c.TTL).Err())

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
		Client:      client,
		Marshaler:   MsgpackMarshaler,
		Unmarshaler: MsgpackUnmarshaler,
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
		Client:      client,
		Marshaler:   MsgpackMarshaler,
		Unmarshaler: MsgpackUnmarshaler,
		DefaultTTL:  time.Second * 2,
	})
	var val string

	ctx := context.Background()

	// Set another value which is not for the cache to make sure it doesn't remove any value!
	require.Nil(t, client.Set(ctx, "another_key", "val", 0).Err())

	require.Nil(t, r.Set(ctx, "k1", "hi"))
	require.Nil(t, r.Set(ctx, "k2", "hi"))

	require.Nil(t, r.Purge(ctx))

	assert.Equal(t, ErrKeyNotFound, r.Get(ctx, "k1", &val))
	assert.Equal(t, ErrKeyNotFound, r.Get(ctx, "k2", &val))
	res, err := client.Get(ctx, "another_key").Result()
	assert.Nil(t, err)
	assert.Equal(t, "val", res)
}
