package repository

import (
	"context"
	"fmt"
	"github.com/go-redis/redis/v8"
	"github.com/go-zookeeper/zk"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
	ampq "github.com/rabbitmq/amqp091-go"
	"github.com/segmentio/kafka-go"
	"log"
	"os"
	"os/exec"
	"testing"
	"time"
)

var (
	dbPool        *pgxpool.Pool
	postgresCache *PostgresCache

	redisClient      redis.UniversalClient
	redisCache       *RedisCache
	redisStreamCache *RedisStreamCache

	kafkaConn  *kafka.Conn
	kafkaCache *KafkaCache

	rabbitConn  *ampq.Connection
	rabbitCache *RabbitCache
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

	rabbitResource := initializeRabbit(dockerPool, newRabbitConfig())
	rabbitCache, err = NewRabbitCache(rabbitConn)
	if err != nil {
		log.Fatal(err)
	}
	netWork, err := dockerPool.Client.CreateNetwork(docker.CreateNetworkOptions{Name: "zookeeper_kafka_network"})
	if err != nil {
		log.Fatalf("could not create a network to zookeeper and kafka: %s", err)
	}
	zooKeeperResource := initializeZooKeeper(dockerPool, newZooKeeper(netWork.ID))
	kafkaResource := initializeKafka(dockerPool, newKafkaConfig(netWork.ID))
	kafkaCache = NewKafkaCache(kafkaConn)

	code := m.Run()

	purgeResources(dockerPool, postgresResource, redisResource, zooKeeperResource, kafkaResource, rabbitResource)
	if err = dockerPool.Client.RemoveNetwork(netWork.ID); err != nil {
		log.Fatalf("could not remove %s network: %s", netWork.Name, err)
	}
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

func initializeZooKeeper(dockerPool *dockertest.Pool, cfg *zooKeeperConfig) *dockertest.Resource {
	zookeeperResource, err := dockerPool.RunWithOptions(&dockertest.RunOptions{
		Name:         cfg.Name,
		Repository:   cfg.Repository,
		Tag:          cfg.Tag,
		NetworkID:    cfg.NetworkID,
		Hostname:     cfg.Hostname,
		ExposedPorts: cfg.ExposedPorts,
	})
	if err != nil {
		log.Fatalf("could not start zookeeper: %s", err)
	}

	conn, _, err := zk.Connect([]string{cfg.getConnectionString(zookeeperResource.GetPort(cfg.ExposedPorts[0]))}, 10*time.Second)
	if err != nil {
		log.Fatalf("could not connect zookeeper: %s", err)
	}
	defer conn.Close()

	retryFn := func() error {
		switch conn.State() {
		case zk.StateHasSession, zk.StateConnected:
			return nil
		default:
			return fmt.Errorf("can't connect to zookeeper")
		}
	}

	if err = dockerPool.Retry(retryFn); err != nil {
		log.Fatalf("could not connect to zookeeper: %s", err)
	}
	return zookeeperResource
}

func initializeRabbit(dockerPool *dockertest.Pool, rCfg *rabbitConfig) *dockertest.Resource {
	rabbitResource, err := dockerPool.RunWithOptions(&dockertest.RunOptions{
		Repository: rCfg.Repository,
		Tag:        rCfg.Tag,
	})
	if err != nil {
		log.Fatal(err)
	}
	err = dockerPool.Retry(func() error {
		rabbitConn, err = ampq.Dial(rCfg.getConnectionString(rabbitResource.GetHostPort(rCfg.PortID)))
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		log.Fatal(err)
	}
	return rabbitResource
}

func initializeKafka(dockerPool *dockertest.Pool, cfg *kafkaConfig) *dockertest.Resource {
	kafkaResource, err := dockerPool.RunWithOptions(&dockertest.RunOptions{
		Name:         cfg.Name,
		Repository:   cfg.Repository,
		Tag:          cfg.Tag,
		NetworkID:    cfg.NetworkID,
		Hostname:     cfg.Hostname,
		Env:          cfg.Env,
		PortBindings: cfg.PortBindings,
		ExposedPorts: cfg.ExposedPorts,
	})
	if err != nil {
		log.Fatalf("could not start kafka: %s", err)
	}
	err = dockerPool.Retry(func() error {
		ctx := context.Background()
		partition := 0
		network, address := cfg.getConnectionString(kafkaResource.GetPort(cfg.ExposedPorts[0]))
		kafkaConn, err = kafka.DialLeader(ctx, network, address, cacheTopic, partition)
		if err != nil {
			return err
		}
		return err
	})
	if err != nil {
		log.Fatal(err)
	}
	return kafkaResource
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

type kafkaConfig struct {
	Name         string
	Repository   string
	Tag          string
	NetworkID    string
	Hostname     string
	Env          []string
	PortBindings map[docker.Port][]docker.PortBinding
	ExposedPorts []string
}

func newKafkaConfig(networkID string) *kafkaConfig {
	return &kafkaConfig{
		Name:       "kafka-example",
		Repository: "wurstmeister/kafka",
		Tag:        "2.13-2.8.1",
		NetworkID:  networkID,
		Hostname:   "kafka",
		Env: []string{
			"KAFKA_CREATE_TOPICS=domain.test:1:1:compact",
			"KAFKA_ADVERTISED_LISTENERS=INSIDE://kafka:9092,OUTSIDE://localhost:9093",
			"KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=INSIDE:PLAINTEXT,OUTSIDE:PLAINTEXT",
			"KAFKA_LISTENERS=INSIDE://0.0.0.0:9092,OUTSIDE://0.0.0.0:9093",
			"KAFKA_ZOOKEEPER_CONNECT=zookeeper:2181",
			"KAFKA_INTER_BROKER_LISTENER_NAME=INSIDE",
			"KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1",
		},
		PortBindings: map[docker.Port][]docker.PortBinding{
			"9093/tcp": {{HostIP: "localhost", HostPort: "9093/tcp"}},
		},
		ExposedPorts: []string{"9093/tcp"},
	}
}

func (f *kafkaConfig) getConnectionString(port string) (string, string) {
	return "tcp", fmt.Sprintf("localhost:%s", port)
}

type zooKeeperConfig struct {
	Name         string
	Repository   string
	Tag          string
	NetworkID    string
	Hostname     string
	ExposedPorts []string
}

func newZooKeeper(networkID string) *zooKeeperConfig {
	return &zooKeeperConfig{
		Name:         "zookeeper-example",
		Repository:   "wurstmeister/zookeeper",
		Tag:          "latest",
		NetworkID:    networkID,
		Hostname:     "zookeeper",
		ExposedPorts: []string{"2181/tcp"},
	}
}

func (*zooKeeperConfig) getConnectionString(port string) string {
	return fmt.Sprintf("127.0.0.1:%s", port)
}

type rabbitConfig struct {
	Repository string
	Tag        string
	PortID     string
}

func newRabbitConfig() *rabbitConfig {
	return &rabbitConfig{
		Repository: "rabbitmq",
		Tag:        "latest",
		PortID:     "5672/tcp",
	}
}

func (*rabbitConfig) getConnectionString(hostAndPort string) string {
	return fmt.Sprintf("amqp://guest:guest@%s/", hostAndPort)
}
