package hcache

import (
	"context"
	"fmt"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/kamva/hexa/hlog"
	"github.com/kamva/tracer"
)

const DELETE_BY_PATTERN_SCRIPT = `
local keys = redis.call('keys', ARGV[1]) 
for i=1,#keys,5000 do
	redis.call('del', unpack(keys, i, math.min(i+4999, #keys)))
end
return keys`

type redisCache struct {
	// prefix is a global prefix
	prefix     string
	name       string
	cli        *redis.Client
	marshal    Marshaler
	unmarshal  Unmarshaler
	defaultTTL time.Duration
}

type RedisOptions struct {
	Prefix      string
	Client      *redis.Client
	Marshaler   Marshaler
	Unmarshaler Unmarshaler
	DefaultTTL  time.Duration
}

func NewRedisCache(name string, o *RedisOptions) Cache {
	return &redisCache{
		prefix:     o.Prefix,
		name:       name,
		cli:        o.Client,
		marshal:    o.Marshaler,
		unmarshal:  o.Unmarshaler,
		defaultTTL: o.DefaultTTL,
	}
}

func (c *redisCache) Name() string {
	return c.name
}

func (c *redisCache) key(k string) string {
	// e.g., cache_user_283jf38jf (prefix is "cache_")
	return fmt.Sprintf("%s%s_%s", c.prefix, c.name, k)
}

func (c *redisCache) Get(ctx context.Context, key string, val interface{}) error {
	b, err := c.cli.Get(ctx, c.key(key)).Bytes()
	if err != nil {
		if err == redis.Nil {
			return ErrKeyNotFound
		}

		return tracer.Trace(err)
	}

	return tracer.Trace(c.unmarshal(b, val))
}

func (c *redisCache) Set(ctx context.Context, key string, val interface{}) error {
	return c.SetWithTTL(ctx, key, val, 0)
}

func (c *redisCache) SetWithDefaultTTL(ctx context.Context, key string, val interface{}) error {
	return c.SetWithTTL(ctx, key, val, c.defaultTTL)
}

func (c *redisCache) SetWithTTL(ctx context.Context, key string, val interface{}, ttl time.Duration) error {
	b, err := c.marshal(val)
	if err != nil {
		return tracer.Trace(err)
	}

	return tracer.Trace(c.cli.Set(ctx, c.key(key), b, ttl).Err())
}

func (c *redisCache) Remove(ctx context.Context, key string) error {
	return tracer.Trace(c.cli.Del(ctx, c.key(key)).Err())
}

func (c *redisCache) Purge(ctx context.Context) error {
	hlog.Warn("purge cache store", hlog.String("name", c.name), hlog.String("prefix", c.prefix))
	return tracer.Trace(c.cli.Eval(ctx, DELETE_BY_PATTERN_SCRIPT, nil, c.key("*")).Err())
}

var _ Cache = &redisCache{}
