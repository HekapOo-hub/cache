package repository

import (
	"context"
	"fmt"
	"github.com/HekapOo-hub/cache/internal/model"
	"github.com/google/uuid"
	ampq "github.com/rabbitmq/amqp091-go"
	"github.com/sirupsen/logrus"
	"sync"
)

const (
	exchange  = "rabbit"
	mandatory = false
	exclusive = false
	immediate = false
	autoAck   = true
	noLocal   = false
	noWait    = false
	key       = ""
)

type RabbitCache struct {
	cacheChan *ampq.Channel
	cacheMU   sync.RWMutex
	cache     map[uuid.UUID]model.User
}

func NewRabbitCache(conn *ampq.Connection) (*RabbitCache, error) {
	cacheChan, err := conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("new rabbit cache: %w", err)
	}
	r := &RabbitCache{cacheChan: cacheChan, cache: make(map[uuid.UUID]model.User)}
	err = r.createCacheExchange()
	if err != nil {
		return nil, fmt.Errorf("new rabbit cache: %w", err)
	}
	return r, nil
}

func (r *RabbitCache) createCacheExchange() error {
	err := r.cacheChan.ExchangeDeclare(exchange, ampq.ExchangeFanout, false, false, false, noWait, nil)
	if err != nil {
		return fmt.Errorf("create cache exchange: %w", err)
	}
	return nil
}

func (r *RabbitCache) Create(ctx context.Context, u *model.User) error {
	encodedUser, err := u.MarshalBinary()
	if err != nil {
		return fmt.Errorf("rabbit cache: create: %w", err)
	}
	err = r.cacheChan.PublishWithContext(ctx, exchange, key, mandatory, immediate,
		ampq.Publishing{ContentType: "text/plain", Body: encodedUser})
	if err != nil {
		return fmt.Errorf("rabbit cache: create: %w", err)
	}
	return nil
}

func (r *RabbitCache) Get(id uuid.UUID) (*model.User, error) {
	r.cacheMU.RLock()
	u, ok := r.cache[id]
	r.cacheMU.RUnlock()
	if !ok {
		return nil, fmt.Errorf("rabbit cache: get: %w", ErrEntityNotFound)
	}
	return &u, nil
}

func (r *RabbitCache) Delete(id uuid.UUID) {
	r.cacheMU.Lock()
	delete(r.cache, id)
	r.cacheMU.Unlock()
}

func (r *RabbitCache) ListenToCreate(ctx context.Context, errChan chan<- error) {
	queueName, err := r.createCacheQueue()
	if err != nil {
		logrus.Info(err)
		errChan <- fmt.Errorf("rabbit cache: listen to create: %w", err)
		return
	}

	messages, err := r.cacheChan.Consume(queueName, "", autoAck, exclusive, noLocal, noWait, nil)
	if err != nil {
		logrus.Info(err)
		errChan <- fmt.Errorf("rabbit cache: listen to create: %w", err)
		return
	}
	for {
		select {
		case <-ctx.Done():
			if err := ctx.Err(); err != nil {
				errChan <- fmt.Errorf("rabbit cache: listen to create: %w", err)
			}
			return
		case msg := <-messages:
			var u model.User
			err = u.UnmarshalBinary(msg.Body)
			if err != nil {
				errChan <- fmt.Errorf("rabbit cache: listen to create: unmarshal: %w", err)
				continue
			}
			r.cacheMU.Lock()
			r.cache[u.ID] = u
			r.cacheMU.Unlock()
		}
	}
}

func (r *RabbitCache) createCacheQueue() (string, error) {
	q, err := r.cacheChan.QueueDeclare(
		"",    // name
		false, // durable
		false, // delete when unused
		true,  // exclusive
		false, // no-wait
		nil,   // arguments
	)
	if err != nil {
		return "", fmt.Errorf("create cache queue: %w", err)
	}
	err = r.cacheChan.QueueBind(
		q.Name,   // queue name
		key,      // routing key
		exchange, // exchange
		false,
		nil,
	)
	if err != nil {
		return "", fmt.Errorf("create cache queue: %w", err)
	}
	return q.Name, nil
}
