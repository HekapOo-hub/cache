package repository

import (
	"context"
	"fmt"
	"github.com/HekapOo-hub/cache/internal/model"
	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"
	"sync"
)

const (
	cacheTopic = "cache_topic"
)

type KafkaCache struct {
	cacheMU sync.RWMutex
	cache   map[uuid.UUID]model.User
	conn    *kafka.Conn
}

func NewKafkaCache(conn *kafka.Conn) *KafkaCache {
	return &KafkaCache{conn: conn, cache: make(map[uuid.UUID]model.User)}
}

func (k *KafkaCache) Create(u *model.User) error {
	encodedUser, err := u.MarshalBinary()
	if err != nil {
		return fmt.Errorf("kafka cache: create: %w", err)
	}
	message := []kafka.Message{
		{
			Key:   []byte(u.ID.String()),
			Value: encodedUser,
		},
	}
	_, err = k.conn.WriteMessages(message...)
	if err != nil {
		return fmt.Errorf("kafka cache: create: %w", err)
	}
	return nil
}

func (k *KafkaCache) Get(id uuid.UUID) (*model.User, error) {
	k.cacheMU.RLock()
	u, ok := k.cache[id]
	k.cacheMU.RUnlock()
	if !ok {
		return nil, fmt.Errorf("kafka cache: get: %w", ErrEntityNotFound)
	}
	return &u, nil
}

func (k *KafkaCache) Delete(id uuid.UUID) {
	k.cacheMU.Lock()
	delete(k.cache, id)
	k.cacheMU.Unlock()
}

func (k *KafkaCache) ListenToCreate(ctx context.Context, errChan chan<- error) {
	brokers, err := k.getBrokers()
	if err != nil {
		errChan <- fmt.Errorf("kafka cache: listen to create: %w", err)
		return
	}
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: brokers,
		Topic:   cacheTopic,
	})
	defer func() {
		err := reader.Close()
		if err != nil {
			errChan <- err
		}
	}()
	for {
		select {
		case <-ctx.Done():
			return
		default:
			msg, err := reader.ReadMessage(ctx)
			if err != nil {
				errChan <- fmt.Errorf("kafka cache: listen to create: %w", err)
				continue
			}
			var u model.User
			err = u.UnmarshalBinary(msg.Value)
			if err != nil {
				errChan <- fmt.Errorf("kafka cache: listen to create: unmarshal: %w", err)
				continue
			}
			k.cacheMU.Lock()
			k.cache[u.ID] = u
			k.cacheMU.Unlock()
		}
	}
}

func (k *KafkaCache) getBrokers() ([]string, error) {
	brokers, err := k.conn.Brokers()
	if err != nil {
		return nil, fmt.Errorf("get brokers: %w", err)
	}
	res := make([]string, 0, len(brokers))
	for _, broker := range brokers {
		addr := fmt.Sprintf("%s:%d", broker.Host, broker.Port)
		res = append(res, addr)
	}
	return res, nil
}
