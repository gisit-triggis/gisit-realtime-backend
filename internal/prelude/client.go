package prelude

import (
	"fmt"
	"github.com/gocql/gocql"
	"github.com/scylladb/gocqlx/v3"
	"go.uber.org/zap"
	"os"
)

func InitClient(logger *zap.Logger) *gocqlx.Session {
	host := os.Getenv("SCYLLADB_HOST")
	if host == "" {
		host = "localhost"
	}

	cluster := gocql.NewCluster(host)

	cluster.Authenticator = gocql.PasswordAuthenticator{
		Username: os.Getenv("SCYLLA_USER"),
		Password: os.Getenv("SCYLLA_PASS"),
	}

	cluster.Keyspace = ""
	cluster.Consistency = gocql.Quorum

	baseSession, err := cluster.CreateSession()
	if err != nil {
		logger.Fatal("Failed to create ScyllaDB session", zap.Error(err))
	}
	defer baseSession.Close()

	keyspace := os.Getenv("SCYLLA_KEYSPACE")
	if keyspace == "" {
		keyspace = "realtime"
	}

	createKeyspaceCQL := fmt.Sprintf(`
		CREATE KEYSPACE IF NOT EXISTS %s
		WITH replication = {
			'class': 'SimpleStrategy',
			'replication_factor': 1
		}
	`, keyspace)

	if err := baseSession.Query(createKeyspaceCQL).Exec(); err != nil {
		logger.Fatal("Failed to create keyspace", zap.Error(err))
	}

	logger.Info("Keyspace ensured", zap.String("keyspace", keyspace))

	cluster.Keyspace = keyspace
	mainSession, err := gocqlx.WrapSession(cluster.CreateSession())
	if err != nil {
		logger.Fatal("Failed to wrap ScyllaDB session", zap.Error(err))
	}

	logger.Info("ScyllaDB connection initialized successfully.")

	return &mainSession
}
