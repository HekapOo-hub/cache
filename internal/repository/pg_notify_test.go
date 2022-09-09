package repository

import (
	"context"
	"github.com/HekapOo-hub/cache/internal/model"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestPostgresCache_GetOnInsert(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	errChan := make(chan error)
	go postgresCache.ListenToNotifications(ctx, errChan)
	time.Sleep(time.Second)
	u := model.User{
		ID:    uuid.New(),
		Name:  "Andrew",
		Age:   12,
		Email: "@gmail.com",
	}
	_, err := dbPool.Exec(ctx, "INSERT INTO users (id, name, age, email) VALUES ($1,$2,$3,$4)",
		u.ID, u.Name, u.Age, u.Email)
	require.NoError(t, err)
	time.Sleep(time.Second)
	actualUser, err := postgresCache.Get(u.ID)
	require.NoError(t, err)
	require.Equal(t, u, *actualUser)
	cancel()
	err = <-errChan
	require.ErrorIs(t, err, context.Canceled)
}

func TestPostgresCache_GetOnUpdate(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	errChan := make(chan error)
	user := model.User{
		ID:    uuid.New(),
		Name:  "Andrew",
		Age:   12,
		Email: "@gmail.com",
	}
	_, err := dbPool.Exec(ctx, "INSERT INTO users (id, name, age, email) VALUES ($1,$2,$3,$4)",
		user.ID, user.Name, user.Age, user.Email)
	go postgresCache.ListenToNotifications(ctx, errChan)
	time.Sleep(time.Second)
	updatedUser := model.User{
		ID:    user.ID,
		Name:  "Dima",
		Age:   15,
		Email: "wiregn@",
	}
	_, err = dbPool.Exec(ctx, "UPDATE users SET name = $1, age = $2, email = $3 WHERE id = $4",
		updatedUser.Name, updatedUser.Age, updatedUser.Email, user.ID)
	require.NoError(t, err)
	time.Sleep(time.Second)
	actualUser, err := postgresCache.Get(user.ID)
	require.NoError(t, err)
	require.Equal(t, updatedUser, *actualUser)
	cancel()
	err = <-errChan
	require.ErrorIs(t, err, context.Canceled)
}
