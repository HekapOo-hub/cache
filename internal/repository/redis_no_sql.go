package repository

import (
	"context"
	"fmt"
	"github.com/HekapOo-hub/cache/internal/model"
	"github.com/go-redis/redis/v8"
	"github.com/google/uuid"
	"time"
)

const (
	expirationTime = 0 * time.Second
)

type RedisCache struct {
	cli redis.UniversalClient
}

func NewRedisCache(redisClient redis.UniversalClient) *RedisCache {
	return &RedisCache{
		cli: redisClient,
	}
}

func (r *RedisCache) Create(ctx context.Context, u *model.User) error {
	err := r.cli.Set(ctx, u.ID.String(), u, expirationTime).Err()
	if err != nil {
		return fmt.Errorf("redis cache: create: %w", err)
	}
	return nil
}

func (r *RedisCache) Get(ctx context.Context, id uuid.UUID) (*model.User, error) {
	var u model.User
	err := r.cli.Get(ctx, id.String()).Scan(&u)
	if err != nil {
		if err == redis.Nil {
			return nil, fmt.Errorf("redis cache: get: %w", ErrEntityNotFound)
		}
		return nil, fmt.Errorf("redis cache: get: %w", err)
	}

	return &u, nil
}

func (r *RedisCache) Delete(ctx context.Context, id uuid.UUID) error {
	err := r.cli.Del(ctx, id.String()).Err()
	if err != nil {
		return fmt.Errorf("redis cache: delete: %w", err)
	}
	return nil
}
