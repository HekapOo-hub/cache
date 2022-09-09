package repository

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/HekapOo-hub/cache/internal/model"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v4/pgxpool"
	"sync"
)

var (
	cacheChannel = "cache_notification"
)

type PostgresCache struct {
	pool    *pgxpool.Pool
	cacheMU sync.RWMutex
	cache   map[uuid.UUID]model.User
}

func NewPostgresCache(pool *pgxpool.Pool) *PostgresCache {
	postgresCache := &PostgresCache{
		pool:  pool,
		cache: make(map[uuid.UUID]model.User),
	}
	return postgresCache
}

func (p *PostgresCache) Get(id uuid.UUID) (*model.User, error) {
	p.cacheMU.RLock()
	u, ok := p.cache[id]
	p.cacheMU.RUnlock()
	if !ok {
		return nil, fmt.Errorf("postgres cache: get: %w", ErrEntityNotFound)
	}
	return &u, nil
}

func (p *PostgresCache) set(id uuid.UUID, u *model.User) {
	p.cacheMU.Lock()
	p.cache[id] = *u
	p.cacheMU.Unlock()
}

func (p *PostgresCache) ListenToNotifications(ctx context.Context, errChan chan<- error) {
	_, err := p.pool.Exec(ctx, fmt.Sprintf("LISTEN %s", cacheChannel))
	if err != nil {
		errChan <- fmt.Errorf("postgres cache: listen to notifications: listen to chan: %w", err)
		return
	}

	conn, err := p.pool.Acquire(ctx)
	if err != nil {
		errChan <- fmt.Errorf("postgres cache: listen to notifications: acquire conn: %w", err)
		return
	}

	for {
		select {
		case <-ctx.Done():
			return
		default:
			notification, err := conn.Conn().WaitForNotification(ctx)
			if err != nil {
				errChan <- fmt.Errorf("postgres cache: listen to notifications: %w", err)
				continue
			}
			var u model.User
			err = json.Unmarshal([]byte(notification.Payload), &u)
			if err != nil {
				errChan <- fmt.Errorf("postgres cache: listen to notifications: unmarshal: %w", err)
				continue
			}
			p.set(u.ID, &u)
		}
	}
}
