package hcache

import (
	"context"

	"github.com/kamva/hexa"
)

type redisCacheProvider struct {
	opts *RedisOptions
}

func NewRedisCacheProvider(opts *RedisOptions) Provider {
	return &redisCacheProvider{opts: opts}
}

func (p *redisCacheProvider) Cache(name string) Cache {
	return NewRedisCache(name, p.opts)
}

func (p *redisCacheProvider) HealthIdentifier() string {
	return "redis_cache_provider"
}

func (p *redisCacheProvider) LivenessStatus(ctx context.Context) hexa.LivenessStatus {
	if p.opts.Client.Ping(ctx).Err() != nil {
		return hexa.StatusDead
	}
	return hexa.StatusAlive
}

func (p *redisCacheProvider) ReadinessStatus(ctx context.Context) hexa.ReadinessStatus {
	if p.opts.Client.Ping(ctx).Err() != nil {
		return hexa.StatusUnReady
	}
	return hexa.StatusReady
}

func (p *redisCacheProvider) HealthStatus(ctx context.Context) hexa.HealthStatus {
	return hexa.HealthStatus{
		Id:    p.HealthIdentifier(),
		Alive: p.LivenessStatus(ctx),
		Ready: p.ReadinessStatus(ctx),
		Tags:  nil,
	}
}

var _ Provider = &redisCacheProvider{}
var _ hexa.Health = &redisCacheProvider{}
