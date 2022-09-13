package consumer

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/HekapOo-hub/cache/internal/model"
	"github.com/HekapOo-hub/cache/internal/producer"
	"github.com/HekapOo-hub/cache/internal/repository"
	"github.com/go-redis/redis/v8"
	"sync"
)

const (
	currentMaxID             = "$"
	noWaitingForMessages     = -1
	lastConsumedIDPattern    = "%s_last_consumed_ID"
	neverDeliveredMessagesID = ">"
)

type messageInfo struct {
	MsgID  string
	Stream string
}

type RedisConsumer struct {
	cli     redis.UniversalClient
	lastIds repository.LastIDStorage

	processingMU sync.RWMutex
	processing   map[model.User]messageInfo
}

func NewRedisConsumer(redisClient redis.UniversalClient, lastIDs repository.LastIDStorage) *RedisConsumer {
	rc := &RedisConsumer{
		cli:        redisClient,
		lastIds:    lastIDs,
		processing: make(map[model.User]messageInfo),
	}
	return rc
}

func (r *RedisConsumer) ReadStreams(ctx context.Context, messagesReceivedPerStream int, consumerChan chan<- model.User, errChan chan<- error, streams ...string) {
	for {
		select {
		case <-ctx.Done():
			if err := ctx.Err(); err != nil {
				errChan <- fmt.Errorf("redis consumer: read streams: %w", err)
			}
			return
		default:
			streamsAndIDs := streams
			for _, stream := range streams {
				lastID, err := r.lastIds.Get(ctx, fmt.Sprintf(lastConsumedIDPattern, stream))
				if err != nil {
					errChan <- fmt.Errorf("redis consumer: read streams: %w", err)
					continue
				}
				streamsAndIDs = append(streamsAndIDs, lastID)
			}

			xStreams, err := r.cli.XRead(ctx, &redis.XReadArgs{
				Streams: streamsAndIDs,
				Count:   int64(messagesReceivedPerStream),
				Block:   noWaitingForMessages,
			}).Result()
			if err != nil {
				errChan <- fmt.Errorf("redis consumer: read streams: xread streams: %w", err)
				continue
			}

			for _, xStream := range xStreams {
				for _, msg := range xStream.Messages {
					var u model.User
					err = json.Unmarshal([]byte(msg.Values[producer.RedisStreamMapKey].(string)), &u)
					if err != nil {
						errChan <- fmt.Errorf("stream cache: listen to create: unmarshal: %w", err)
						continue
					}
					consumerChan <- u
					r.markAsProcessing(&u, xStream.Stream, msg.ID)
				}
			}
		}
	}
}

func (r *RedisConsumer) markAsProcessing(u *model.User, stream, msgID string) {
	r.processingMU.Lock()
	r.processing[*u] = messageInfo{
		Stream: stream,
		MsgID:  msgID,
	}
	r.processingMU.Unlock()
}

func (r *RedisConsumer) MarkAsProcessed(ctx context.Context, user *model.User) error {
	r.processingMU.Lock()
	info, ok := r.processing[*user]
	delete(r.processing, *user)
	r.processingMU.Unlock()
	if !ok {
		return fmt.Errorf("%v was already marked as processed", *user)
	}
	err := r.lastIds.Set(ctx, fmt.Sprintf(lastConsumedIDPattern, info.Stream), info.MsgID)
	if err != nil {
		return fmt.Errorf("redis consumer: read streams: %w", err)
	}
	return nil
}
