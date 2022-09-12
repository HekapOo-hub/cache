package repository

import (
	"context"
	"github.com/HekapOo-hub/cache/internal/model"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestKafkaCache_Create(t *testing.T) {
	errChan := make(chan error)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go kafkaCache.ListenToCreate(ctx, errChan)
	u := model.User{
		ID:    uuid.New(),
		Name:  "kafka_user",
		Age:   11,
		Email: "fafka@gmailc.com",
	}
	err := kafkaCache.Create(&u)
	require.NoError(t, err)
	time.Sleep(time.Second)
	actualUser, err := kafkaCache.Get(u.ID)
	require.NoError(t, err)

	require.Equal(t, u, *actualUser)
	cancel()
	require.ErrorIs(t, <-errChan, context.Canceled)
}

func TestKafkaCache_Sync(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errChan := make(chan error)
	kafkaCacheCpy := NewKafkaCache(kafkaConn)
	go kafkaCacheCpy.ListenToCreate(ctx, errChan)
	u := model.User{
		ID:    uuid.New(),
		Name:  "Stream",
		Age:   125,
		Email: "@gmail.com",
	}
	err := kafkaCache.Create(&u)
	require.NoError(t, err)
	time.Sleep(time.Second)

	actualUser, err := kafkaCacheCpy.Get(u.ID)
	require.NoError(t, err)
	require.Equal(t, u, *actualUser)
	cancel()
	err = <-errChan
	require.ErrorIs(t, err, context.Canceled)
}

func TestKafkaCache_Delete(t *testing.T) {
	errChan := make(chan error)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go kafkaCache.ListenToCreate(ctx, errChan)
	u := model.User{
		ID:    uuid.New(),
		Name:  "kafka_user",
		Age:   11,
		Email: "fafka@gmailc.com",
	}
	err := kafkaCache.Create(&u)
	require.NoError(t, err)
	time.Sleep(time.Second)
	err = kafkaCache.Delete(u.ID)
	require.NoError(t, err)
	_, err = kafkaCache.Get(u.ID)
	require.ErrorIs(t, err, ErrEntityNotFound)

	cancel()
	require.ErrorIs(t, <-errChan, context.Canceled)
}
