package repository

import (
	"context"
	"github.com/HekapOo-hub/cache/internal/model"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestRedisStreamCache_Create(t *testing.T) {
	errChan := make(chan error)
	ctx, cancel := context.WithCancel(context.Background())
	go redisStreamCache.ListenToCreate(ctx, errChan)
	u := model.User{
		ID:    uuid.New(),
		Name:  "Stream",
		Age:   125,
		Email: "@gmail.com",
	}
	err := redisStreamCache.Create(ctx, &u)
	require.NoError(t, err)
	time.Sleep(time.Second)
	actualUser, err := redisStreamCache.Get(u.ID)
	require.NoError(t, err)
	require.Equal(t, u, *actualUser)

	cancel()
	require.ErrorIs(t, <-errChan, context.Canceled)
}

func TestRedisStreamCache_Sync(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	errChan := make(chan error)
	redisStreamCacheCpy := NewRedisStreamCache(redisClient)
	go redisStreamCacheCpy.ListenToCreate(ctx, errChan)
	go redisStreamCache.ListenToCreate(ctx, errChan)

	u := model.User{
		ID:    uuid.New(),
		Name:  "Stream",
		Age:   125,
		Email: "@gmail.com",
	}
	err := redisStreamCache.Create(ctx, &u)
	require.NoError(t, err)
	time.Sleep(time.Second)

	actualUserCpy, err := redisStreamCacheCpy.Get(u.ID)
	require.NoError(t, err)
	require.Equal(t, u, *actualUserCpy)

	actualUser, err := redisStreamCache.Get(u.ID)
	require.NoError(t, err)
	require.Equal(t, u, *actualUser)
	cancel()
	require.ErrorIs(t, <-errChan, context.Canceled)
	require.ErrorIs(t, <-errChan, context.Canceled)
}

func TestRedisStreamCache_Delete(t *testing.T) {
	errChan := make(chan error)
	ctx, cancel := context.WithCancel(context.Background())
	go redisStreamCache.ListenToCreate(ctx, errChan)
	u := model.User{
		ID:    uuid.New(),
		Name:  "Stream",
		Age:   125,
		Email: "@gmail.com",
	}
	err := redisStreamCache.Create(ctx, &u)
	require.NoError(t, err)
	time.Sleep(time.Second)
	redisStreamCache.Delete(u.ID)

	_, err = redisStreamCache.Get(u.ID)
	require.ErrorIs(t, err, ErrEntityNotFound)

	cancel()
	require.ErrorIs(t, <-errChan, context.Canceled)
}
