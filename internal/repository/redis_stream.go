package repository

import (
	"context"
	"errors"
	"fmt"
	"github.com/HekapOo-hub/cache/internal/model"
	"github.com/go-redis/redis/v8"
	"github.com/google/uuid"
	"sync"
)

const (
	redisStreamMapKey         = "user"
	redisStreamCacheName      = "myStream"
	messagesReceivedPerStream = 1
	noWaitingForMessages      = -1
)

var ErrEntityNotFound = errors.New("entity not found")

type RedisStreamCache struct {
	lastID  string
	cacheMU sync.RWMutex
	cache   map[uuid.UUID]model.User
	cli     redis.UniversalClient
}

func NewRedisStreamCache(redisCli redis.UniversalClient) *RedisStreamCache {
	return &RedisStreamCache{cli: redisCli, lastID: "0-0", cache: make(map[uuid.UUID]model.User)}
}

func (s *RedisStreamCache) Create(ctx context.Context, u *model.User) error {
	err := s.cli.XAdd(ctx, &redis.XAddArgs{
		Stream: redisStreamCacheName,
		Values: map[string]interface{}{
			redisStreamMapKey: u,
		},
	}).Err()
	if err != nil {
		return fmt.Errorf("stream cache: create: %w", err)
	}
	return nil
}

func (s *RedisStreamCache) Get(id uuid.UUID) (*model.User, error) {
	s.cacheMU.RLock()
	u, ok := s.cache[id]
	s.cacheMU.RUnlock()
	if !ok {
		return nil, ErrEntityNotFound
	}
	return &u, nil
}

func (s *RedisStreamCache) Delete(id uuid.UUID) {
	s.cacheMU.Lock()
	delete(s.cache, id)
	s.cacheMU.Unlock()
}

func (s *RedisStreamCache) ListenToCreate(ctx context.Context, errChan chan<- error) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			res, err := s.cli.XRead(ctx, &redis.XReadArgs{
				Block:   noWaitingForMessages,
				Count:   messagesReceivedPerStream,
				Streams: []string{redisStreamCacheName, s.lastID},
			}).Result()
			if err != nil {
				if !errors.Is(err, redis.Nil) {
					errChan <- fmt.Errorf("stream cache: listen to create: %w", err)
				}
				continue
			}
			for i := 0; i < messagesReceivedPerStream; i++ {
				// index = 0 because we listen only to one stream
				msg := res[0].Messages[i]
				var u model.User
				err = u.UnmarshalBinary([]byte(msg.Values[redisStreamMapKey].(string)))
				if err != nil {
					errChan <- fmt.Errorf("stream cache: listen to create: binary encode: %w", err)
					continue
				}
				s.cacheMU.Lock()
				s.cache[u.ID] = u
				s.cacheMU.Unlock()
				s.lastID = msg.ID
			}
		}
	}
}
