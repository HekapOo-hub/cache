package producer

import (
	"cache/internal/model"
	"context"
	"fmt"
	"github.com/go-redis/redis/v8"
)

const (
	StreamName1       = "redisStream1"
	StreamName2       = "redisStream2"
	StreamName3       = "redisStream3"
	RedisStreamMapKey = "user"
)

type RedisProducer struct {
	cli redis.UniversalClient
}

func NewRedisProducer(redisClient redis.UniversalClient) *RedisProducer {
	return &RedisProducer{cli: redisClient}
}

func (r *RedisProducer) Send(ctx context.Context, user *model.User, streamName string) error {
	err := r.cli.XAdd(ctx, &redis.XAddArgs{
		Stream: streamName,
		Values: map[string]interface{}{
			RedisStreamMapKey: *user,
		},
	}).Err()
	if err != nil {
		return fmt.Errorf("send: %w", err)
	}
	return nil
}
