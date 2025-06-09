package sqlserver

import (
	"fmt"
	"strconv"
	"time"

	contractsdocker "github.com/goravel/framework/contracts/testing/docker"
	supportdocker "github.com/goravel/framework/support/docker"
	testingdocker "github.com/goravel/framework/testing/docker"
	"github.com/goravel/sqlserver/contracts"
	"github.com/spf13/cast"
	"gorm.io/driver/sqlserver"
	gormio "gorm.io/gorm"
)

type Docker struct {
	config         contracts.ConfigBuilder
	databaseConfig contractsdocker.DatabaseConfig
	imageDriver    contractsdocker.ImageDriver
}

func NewDocker(config contracts.ConfigBuilder, database, username, password string) *Docker {
	return &Docker{
		config: config,
		databaseConfig: contractsdocker.DatabaseConfig{
			Database: database,
			Driver:   Name,
			Host:     "127.0.0.1",
			Password: password,
			Port:     1433,
			Username: username,
		},
		imageDriver: testingdocker.NewImageDriver(contractsdocker.Image{
			Repository: "mcr.microsoft.com/mssql/server",
			Tag:        "latest",
			Env: []string{
				"ACCEPT_EULA=Y",
				"MSSQL_SA_PASSWORD=" + password,
			},
			ExposedPorts: []string{"1433"},
		}),
	}
}

func (r *Docker) Build() error {
	if err := r.imageDriver.Build(); err != nil {
		return err
	}

	config := r.imageDriver.Config()
	r.databaseConfig.ContainerID = config.ContainerID
	r.databaseConfig.Port = cast.ToInt(supportdocker.ExposedPort(config.ExposedPorts, strconv.Itoa(r.databaseConfig.Port)))

	return nil
}

func (r *Docker) Config() contractsdocker.DatabaseConfig {
	return r.databaseConfig
}

func (r *Docker) Database(name string) (contractsdocker.DatabaseDriver, error) {
	docker := NewDocker(r.config, name, r.databaseConfig.Username, r.databaseConfig.Password)
	docker.databaseConfig.ContainerID = r.databaseConfig.ContainerID
	docker.databaseConfig.Port = r.databaseConfig.Port

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

func (r *Docker) Image(image contractsdocker.Image) {
	r.imageDriver = testingdocker.NewImageDriver(image)
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
	r.databaseConfig.ContainerID = containerID
	r.databaseConfig.Port = port

	return nil
}

func (r *Docker) Shutdown() error {
	return r.imageDriver.Shutdown()
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
				"sa", r.databaseConfig.Password, r.databaseConfig.Host, r.databaseConfig.Port),
		}))

		if err == nil {
			// Check if database exists
			var exists bool
			query := fmt.Sprintf("SELECT CASE WHEN EXISTS (SELECT * FROM sys.databases WHERE name = '%s') THEN CAST(1 AS BIT) ELSE CAST(0 AS BIT) END", r.databaseConfig.Database)
			if err := instance.Raw(query).Scan(&exists).Error; err != nil {
				return nil, err
			}

			if !exists {
				// Create User database
				if err := instance.Exec(fmt.Sprintf(`CREATE DATABASE "%s";`, r.databaseConfig.Database)).Error; err != nil {
					return nil, err
				}

				if err := instance.Exec(fmt.Sprintf(`
IF NOT EXISTS (SELECT 1 FROM sys.server_principals WHERE name = '%s' AND type = 'S')
BEGIN
    CREATE LOGIN %s WITH PASSWORD = '%s';
END
				`, r.databaseConfig.Username, r.databaseConfig.Username, r.databaseConfig.Password)).Error; err != nil {
					return nil, err
				}

				// Create DB account for User
				if err := instance.Exec(fmt.Sprintf("USE %s; CREATE USER %s FOR LOGIN %s", r.databaseConfig.Database, r.databaseConfig.Username, r.databaseConfig.Username)).Error; err != nil {
					return nil, err
				}

				// Add permission
				if err := instance.Exec(fmt.Sprintf("USE %s; ALTER ROLE db_owner ADD MEMBER %s", r.databaseConfig.Database, r.databaseConfig.Username)).Error; err != nil {
					return nil, err
				}
			}

			instance, err = gormio.Open(sqlserver.New(sqlserver.Config{
				DSN: fmt.Sprintf("sqlserver://%s:%s@%s:%d?database=%s",
					r.databaseConfig.Username, r.databaseConfig.Password, r.databaseConfig.Host, r.databaseConfig.Port, r.databaseConfig.Database),
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
		writeConfigs[0].Port = r.databaseConfig.Port
		r.config.Config().Add(fmt.Sprintf("database.connections.%s.write", r.config.Connection()), writeConfigs)

		return
	}

	r.config.Config().Add(fmt.Sprintf("database.connections.%s.port", r.config.Connection()), r.databaseConfig.Port)
}
