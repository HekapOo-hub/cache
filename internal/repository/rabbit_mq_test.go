package repository

import (
	"context"
	"github.com/HekapOo-hub/cache/internal/model"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestRabbitCache_Create(t *testing.T) {
	errChan := make(chan error)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go rabbitCache.ListenToCreate(ctx, errChan)

	u := model.User{
		ID:    uuid.New(),
		Name:  "rabbit_user",
		Age:   11,
		Email: "fafka@gmailc.com",
	}
	time.Sleep(time.Second)
	err := rabbitCache.Create(ctx, &u)
	require.NoError(t, err)
	time.Sleep(time.Second)

	actualUser, err := rabbitCache.Get(u.ID)
	require.NoError(t, err)
	require.Equal(t, u, *actualUser)

	cancel()
	require.ErrorIs(t, <-errChan, context.Canceled)
}

func TestRabbitCache_Sync(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errChan := make(chan error)
	rabbitCacheCpy, err := NewRabbitCache(rabbitConn)
	require.NoError(t, err)
	go rabbitCache.ListenToCreate(ctx, errChan)
	go rabbitCacheCpy.ListenToCreate(ctx, errChan)
	time.Sleep(time.Second)
	u := model.User{
		ID:    uuid.New(),
		Name:  "Stream",
		Age:   125,
		Email: "@gmail.com",
	}
	err = rabbitCache.Create(ctx, &u)
	require.NoError(t, err)
	time.Sleep(time.Second)

	actualUserCpy, err := rabbitCacheCpy.Get(u.ID)
	require.NoError(t, err)
	require.Equal(t, u, *actualUserCpy)

	actualUser, err := rabbitCache.Get(u.ID)
	require.NoError(t, err)
	require.Equal(t, u, *actualUser)
	cancel()
	require.ErrorIs(t, <-errChan, context.Canceled)
	require.ErrorIs(t, <-errChan, context.Canceled)
}

func TestRabbitCache_Delete(t *testing.T) {
	errChan := make(chan error)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go rabbitCache.ListenToCreate(ctx, errChan)
	u := model.User{
		ID:    uuid.New(),
		Name:  "kafka_user",
		Age:   11,
		Email: "fafka@gmailc.com",
	}
	time.Sleep(time.Second)
	err := rabbitCache.Create(ctx, &u)
	require.NoError(t, err)
	time.Sleep(time.Second)
	rabbitCache.Delete(u.ID)
	_, err = rabbitCache.Get(u.ID)
	require.ErrorIs(t, err, ErrEntityNotFound)

	cancel()
	require.ErrorIs(t, <-errChan, context.Canceled)
}
