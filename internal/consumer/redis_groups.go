package consumer

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/HekapOo-hub/cache/internal/model"
	"github.com/HekapOo-hub/cache/internal/producer"
	"github.com/go-redis/redis/v8"
	"sync"
)

type messageFromGroupInfo struct {
	MsgID  string
	Stream string
	Group  string
}

type RedisGroupConsumer struct {
	cli redis.UniversalClient

	processingMU sync.RWMutex
	processing   map[model.User]messageFromGroupInfo
}

func NewRedisGroupConsumer(redisClient redis.UniversalClient) *RedisGroupConsumer {
	return &RedisGroupConsumer{
		cli:        redisClient,
		processing: make(map[model.User]messageFromGroupInfo),
	}
}

func (r *RedisGroupConsumer) ReadGroup(ctx context.Context, messagesReceivedPerStream int, consumerChan chan<- model.User,
	errChan chan<- error, consumer, group string, streams ...string) {
	streamsAndIDs := streams
	for i := 0; i < len(streams); i++ {
		streamsAndIDs = append(streamsAndIDs, neverDeliveredMessagesID)
	}
	for {
		select {
		case <-ctx.Done():
			if err := ctx.Err(); err != nil {
				errChan <- fmt.Errorf("redis consumer: read streams: %w", err)
			}
			return
		default:
			xStreams, err := r.cli.XReadGroup(ctx, &redis.XReadGroupArgs{
				Group:    group,
				Consumer: consumer,
				Count:    int64(messagesReceivedPerStream),
				Block:    noWaitingForMessages,
				Streams:  streamsAndIDs,
			}).Result()
			if err != nil {
				errChan <- fmt.Errorf("redis consumer: read group: %w", err)
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
					r.markAsProcessing(&u, xStream.Stream, group, msg.ID)
				}
			}
		}
	}
}

func (r *RedisGroupConsumer) markAsProcessing(u *model.User, stream, group, msgID string) {
	r.processingMU.Lock()
	r.processing[*u] = messageFromGroupInfo{
		Stream: stream,
		MsgID:  msgID,
		Group:  group,
	}
	r.processingMU.Unlock()
}

func (r *RedisGroupConsumer) CreateGroupIfNotExist(ctx context.Context, group, stream string) error {
	groupsInfo, err := r.cli.XInfoGroups(ctx, stream).Result()
	if err != nil {
		return fmt.Errorf("redis producer: send to group: get groups info: %w", err)
	}

	if !groupAlreadyExists(groupsInfo, group) {
		err = r.createGroup(ctx, stream, group)
		if err != nil {
			return fmt.Errorf("redis producer: send to group: %w", err)
		}
	}
	return nil
}

func (r *RedisGroupConsumer) createGroup(ctx context.Context, stream, group string) error {
	err := r.cli.XGroupCreate(ctx, stream, group, currentMaxID).Err()
	if err != nil {
		return fmt.Errorf("create group: creating group: %w", err)
	}
	return nil
}

func groupAlreadyExists(groupsInfo []redis.XInfoGroup, group string) bool {
	for i := range groupsInfo {
		if groupsInfo[i].Name == group {
			return true
		}
	}
	return false
}

func (r *RedisGroupConsumer) MarkAsProcessed(ctx context.Context, user *model.User) error {
	r.processingMU.Lock()
	info, ok := r.processing[*user]
	delete(r.processing, *user)
	r.processingMU.Unlock()
	if !ok {
		return fmt.Errorf("%v was already marked as processed by group", *user)
	}
	err := r.cli.XAck(ctx, info.Stream, info.Group, info.MsgID).Err()
	if err != nil {
		return fmt.Errorf("redis consumer: mark as processed by group: %w", err)
	}
	return nil
}
