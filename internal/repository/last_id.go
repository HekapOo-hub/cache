package repository

import (
	"context"
	"errors"
	"fmt"
	"github.com/go-redis/redis/v8"
)

const (
	currentMaxID = "$"
	noExpiration = -1
)

type LastIDStorage interface {
	Set(ctx context.Context, stream, lastID string) error
	Get(ctx context.Context, stream string) (string, error)
}

type lastIDStorage struct {
	cli redis.UniversalClient
}

func NewLastIDStorage(redisClient redis.UniversalClient) *lastIDStorage {
	lastIdStorage := lastIDStorage{
		cli: redisClient,
	}
	return &lastIdStorage
}

func (l *lastIDStorage) Get(ctx context.Context, stream string) (string, error) {
	lastID, err := l.cli.Get(ctx, stream).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return currentMaxID, nil
		}
		return "", fmt.Errorf("lastIDStorage: get: %w", err)
	}
	return lastID, nil
}

func (l *lastIDStorage) Set(ctx context.Context, stream, lastID string) error {
	err := l.cli.Set(ctx, stream, lastID, noExpiration).Err()
	if err != nil {
		return fmt.Errorf("lastIDStorage: set: %w", err)
	}
	return nil
}
