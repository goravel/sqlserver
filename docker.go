package sqlserver

import (
	"fmt"
	"time"

	"github.com/goravel/framework/contracts/testing"
	"github.com/goravel/framework/support/docker"
	"github.com/goravel/framework/support/process"
	"github.com/goravel/sqlserver/contracts"
	"gorm.io/driver/sqlserver"
	gormio "gorm.io/gorm"
)

type Docker struct {
	config      contracts.ConfigBuilder
	containerID string
	database    string
	host        string
	image       *testing.Image
	password    string
	username    string
	port        int
}

func NewDocker(config contracts.ConfigBuilder, database, username, password string) *Docker {
	return &Docker{
		config:   config,
		database: database,
		host:     "127.0.0.1",
		username: username,
		password: password,
		image: &testing.Image{
			Repository: "mcr.microsoft.com/mssql/server",
			Tag:        "latest",
			Env: []string{
				"ACCEPT_EULA=Y",
				"MSSQL_SA_PASSWORD=" + password,
			},
			ExposedPorts: []string{"1433"},
		},
	}
}

func (r *Docker) Build() error {
	command, exposedPorts := docker.ImageToCommand(r.image)
	containerID, err := process.Run(command)
	if err != nil {
		return fmt.Errorf("init Sqlserver error: %v", err)
	}
	if containerID == "" {
		return fmt.Errorf("no container id return when creating Sqlserver docker")
	}

	r.containerID = containerID
	r.port = docker.ExposedPort(exposedPorts, 1433)

	return nil
}

func (r *Docker) Config() testing.DatabaseConfig {
	return testing.DatabaseConfig{
		ContainerID: r.containerID,
		Host:        r.host,
		Port:        r.port,
		Database:    r.database,
		Username:    r.username,
		Password:    r.password,
	}
}

func (r *Docker) Database(name string) (testing.DatabaseDriver, error) {
	docker := NewDocker(r.config, name, r.username, r.password)
	docker.containerID = r.containerID
	docker.port = r.port

	return docker, nil
}

func (r *Docker) Driver() string {
	return Name
}

func (r *Docker) Fresh() error {
	instance, err := r.connect()
	if err != nil {
		return fmt.Errorf("connect Sqlserver error when clearing: %v", err)
	}

	res := instance.Raw("SELECT NAME FROM SYSOBJECTS WHERE TYPE='U';")
	if res.Error != nil {
		return fmt.Errorf("get tables of Sqlserver error: %v", res.Error)
	}

	var tables []string
	res = res.Scan(&tables)
	if res.Error != nil {
		return fmt.Errorf("get tables of Sqlserver error: %v", res.Error)
	}

	for _, table := range tables {
		res = instance.Exec(fmt.Sprintf("drop table %s;", table))
		if res.Error != nil {
			return fmt.Errorf("drop table %s of Sqlserver error: %v", table, res.Error)
		}
	}

	return r.close(instance)
}

func (r *Docker) Image(image testing.Image) {
	r.image = &image
}

func (r *Docker) Ready() error {
	gormDB, err := r.connect()
	if err != nil {
		return err
	}

	r.resetConfigPort()

	return r.close(gormDB)
}

func (r *Docker) Reuse(containerID string, port int) error {
	r.containerID = containerID
	r.port = port

	return nil
}

func (r *Docker) Shutdown() error {
	if _, err := process.Run(fmt.Sprintf("docker stop %s", r.containerID)); err != nil {
		return fmt.Errorf("stop Sqlserver error: %v", err)
	}

	return nil
}

func (r *Docker) connect() (*gormio.DB, error) {
	var (
		instance *gormio.DB
		err      error
	)

	// docker compose need time to start
	for i := 0; i < 100; i++ {
		instance, err = gormio.Open(sqlserver.New(sqlserver.Config{
			DSN: fmt.Sprintf("sqlserver://%s:%s@%s:%d?database=master",
				"sa", r.password, r.host, r.port),
		}))

		if err == nil {
			// Check if database exists
			var exists bool
			query := fmt.Sprintf("SELECT CASE WHEN EXISTS (SELECT * FROM sys.databases WHERE name = '%s') THEN CAST(1 AS BIT) ELSE CAST(0 AS BIT) END", r.database)
			if err := instance.Raw(query).Scan(&exists).Error; err != nil {
				return nil, err
			}

			if !exists {
				// Create User database
				if err := instance.Exec(fmt.Sprintf(`CREATE DATABASE "%s";`, r.database)).Error; err != nil {
					return nil, err
				}

				query = fmt.Sprintf("SELECT 1 FROM sys.server_principals WHERE name = '%s' AND type = 'S'", r.username)
				if err := instance.Raw(query).Scan(&exists).Error; err != nil {
					return nil, err
				}

				if !exists {
					// Create User account
					if err := instance.Exec(fmt.Sprintf("CREATE LOGIN %s WITH PASSWORD = '%s'", r.username, r.password)).Error; err != nil {
						return nil, err
					}
				}

				// Create DB account for User
				if err := instance.Exec(fmt.Sprintf("USE %s; CREATE USER %s FOR LOGIN %s", r.database, r.username, r.username)).Error; err != nil {
					return nil, err
				}

				// Add permission
				if err := instance.Exec(fmt.Sprintf("USE %s; ALTER ROLE db_owner ADD MEMBER %s", r.database, r.username)).Error; err != nil {
					return nil, err
				}
			}

			instance, err = gormio.Open(sqlserver.New(sqlserver.Config{
				DSN: fmt.Sprintf("sqlserver://%s:%s@%s:%d?database=%s",
					r.username, r.password, r.host, r.port, r.database),
			}))

			break
		}

		time.Sleep(1 * time.Second)
	}

	return instance, err
}

func (r *Docker) close(gormDB *gormio.DB) error {
	db, err := gormDB.DB()
	if err != nil {
		return err
	}

	return db.Close()
}

func (r *Docker) resetConfigPort() {
	writers := r.config.Config().Get(fmt.Sprintf("database.connections.%s.write", r.config.Connection()))
	if writeConfigs, ok := writers.([]contracts.Config); ok {
		writeConfigs[0].Port = r.port
		r.config.Config().Add(fmt.Sprintf("database.connections.%s.write", r.config.Connection()), writeConfigs)

		return
	}

	r.config.Config().Add(fmt.Sprintf("database.connections.%s.port", r.config.Connection()), r.port)
}
