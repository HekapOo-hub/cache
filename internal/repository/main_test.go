package repository

import (
	"context"
	"fmt"
	"github.com/go-redis/redis/v8"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
	"log"
	"os"
	"os/exec"
	"testing"
)

var (
	dbPool        *pgxpool.Pool
	postgresCache *PostgresCache

	redisClient      redis.UniversalClient
	redisCache       *RedisCache
	redisStreamCache *RedisStreamCache
)

func TestMain(m *testing.M) {
	dockerPool, err := dockertest.NewPool("")
	if err != nil {
		log.Fatalf("Could not connect to docker: %s", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	postgresResource := initializePostgres(ctx, dockerPool, newPostgresConfig())
	postgresCache = NewPostgresCache(dbPool)

	redisResource := initializeRedis(ctx, dockerPool, newRedisConfig())
	redisCache = NewRedisCache(redisClient)
	redisStreamCache = NewRedisStreamCache(redisClient)

	code := m.Run()

	purgeResources(dockerPool, postgresResource, redisResource)

	os.Exit(code)
}

func initializePostgres(ctx context.Context, dockerPool *dockertest.Pool, cfg *postgresConfig) *dockertest.Resource {
	resource, err := dockerPool.Run(cfg.Repository, cfg.Version, cfg.EnvVariables)
	if err != nil {
		log.Fatalf("Could not start resource: %s", err)
	}

	var dbHostAndPort string

	err = dockerPool.Retry(func() error {
		dbHostAndPort = resource.GetHostPort(cfg.PortID)

		dbPool, err = pgxpool.Connect(ctx, cfg.getConnectionString(dbHostAndPort))
		if err != nil {
			return err
		}

		return dbPool.Ping(ctx)
	})
	if err != nil {
		log.Fatalf("Could not connect to database: %s", err)
	}

	cmd := exec.Command("flyway", cfg.getFlywayMigrationArgs(dbHostAndPort)...)

	err = cmd.Run()
	if err != nil {
		log.Fatalf("There are errors in migrations: %v", err)
	}
	return resource
}

func initializeRedis(ctx context.Context, dockerPool *dockertest.Pool, rConfig *redisConfig) *dockertest.Resource {
	redisResource, err := dockerPool.RunWithOptions(&dockertest.RunOptions{
		Repository: rConfig.Repository,
		Tag:        rConfig.Version,
	}, func(cfg *docker.HostConfig) {
		cfg.AutoRemove = rConfig.AutoRemove
		cfg.RestartPolicy = docker.RestartPolicy{
			Name: rConfig.RestartPolicy,
		}
	})
	if err != nil {
		log.Fatal(err)
	}

	if err = dockerPool.Retry(func() error {
		cs := rConfig.getConnectionString(redisResource.GetHostPort(rConfig.PortID))

		opts, err := redis.ParseURL(cs)
		if err != nil {
			return err
		}
		redisClient = redis.NewClient(opts)

		_, err = redisClient.Ping(ctx).Result()
		if err != nil {
			return fmt.Errorf("can't connect to redis: %v", err)
		}
		return nil
	}); err != nil {
		log.Fatal(err)
	}
	return redisResource
}

func purgeResources(dockerPool *dockertest.Pool, resources ...*dockertest.Resource) {
	for i := range resources {
		if err := dockerPool.Purge(resources[i]); err != nil {
			log.Fatalf("Could not purge resource: %s", err)
		}

		err := resources[i].Expire(1)
		if err != nil {
			log.Fatal(err)
		}
	}

}

type postgresConfig struct {
	Repository   string
	Version      string
	EnvVariables []string
	PortID       string
}

func newPostgresConfig() *postgresConfig {
	return &postgresConfig{
		Repository:   "postgres",
		Version:      "14.1-alpine",
		EnvVariables: []string{"POSTGRES_PASSWORD=password123"},
		PortID:       "5432/tcp",
	}
}

func (p *postgresConfig) getConnectionString(dbHostAndPort string) string {
	return fmt.Sprintf("postgresql://postgres:password123@%v/%s", dbHostAndPort, p.Repository)
}

func (p *postgresConfig) getFlywayMigrationArgs(dbHostAndPort string) []string {
	return []string{
		"-user=postgres",
		"-password=password123",
		"-locations=filesystem:../../migrations",
		fmt.Sprintf("-url=jdbc:postgresql://%v/postgres", dbHostAndPort),
		"migrate",
	}
}

type redisConfig struct {
	Repository    string
	DB            int
	Version       string
	PortID        string
	RestartPolicy string
	AutoRemove    bool
}

func newRedisConfig() *redisConfig {
	return &redisConfig{
		Repository:    "redis",
		DB:            0,
		Version:       "7-alpine",
		PortID:        "6379/tcp",
		RestartPolicy: "no",
		AutoRemove:    true,
	}
}

func (r *redisConfig) getConnectionString(dbHostAndPort string) string {
	return fmt.Sprintf("%s://%s/%d", r.Repository,
		dbHostAndPort,
		r.DB,
	)
}
