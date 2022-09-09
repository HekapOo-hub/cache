package repository

import (
	"context"
	"github.com/HekapOo-hub/cache/internal/model"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestRedisCache_Create(t *testing.T) {
	user := model.User{
		ID:    uuid.New(),
		Name:  "sasha",
		Age:   321,
		Email: "@wsjg.com",
	}
	ctx := context.Background()

	err := redisCache.Create(ctx, &user)
	require.NoError(t, err)

	actualUser, err := redisCache.Get(ctx, user.ID)
	require.NoError(t, err)
	require.Equal(t, user, *actualUser)
}

func TestRedisCache_Expiration(t *testing.T) {
	if expirationTime == 0 {
		return
	}
	user := model.User{
		ID:    uuid.New(),
		Name:  "sasha",
		Age:   321,
		Email: "@wsjg.com",
	}
	ctx := context.Background()

	err := redisCache.Create(ctx, &user)
	require.NoError(t, err)
	time.Sleep(expirationTime)

	_, err = redisCache.Get(ctx, user.ID)
	require.ErrorIs(t, err, ErrEntityNotFound)
}

func TestRedisCache_Delete(t *testing.T) {
	user := model.User{
		ID:    uuid.New(),
		Name:  "sasha",
		Age:   321,
		Email: "@wsjg.com",
	}
	ctx := context.Background()

	err := redisCache.Create(ctx, &user)
	require.NoError(t, err)

	err = redisCache.Delete(ctx, user.ID)
	require.NoError(t, err)

	_, err = redisCache.Get(ctx, user.ID)
	require.ErrorIs(t, err, ErrEntityNotFound)
}
