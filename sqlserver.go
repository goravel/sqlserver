package sqlserver

import (
	"fmt"

	"github.com/goravel/framework/contracts/config"
	"github.com/goravel/framework/contracts/database"
	"github.com/goravel/framework/contracts/database/driver"
	"github.com/goravel/framework/contracts/log"
	"github.com/goravel/framework/contracts/testing/docker"
	"github.com/goravel/framework/errors"
	"gorm.io/driver/sqlserver"
	"gorm.io/gorm"

	"github.com/goravel/sqlserver/contracts"
)

var _ driver.Driver = &Sqlserver{}

type Sqlserver struct {
	config contracts.ConfigBuilder
	log    log.Log
}

func NewSqlserver(config config.Config, log log.Log, connection string) *Sqlserver {
	return &Sqlserver{
		config: NewConfig(config, connection),
		log:    log,
	}
}

func (r *Sqlserver) Docker() (docker.DatabaseDriver, error) {
	writers := r.config.Writers()
	if len(writers) == 0 {
		return nil, errors.DatabaseConfigNotFound
	}

	return NewDocker(r.config, writers[0].Database, writers[0].Username, writers[0].Password), nil
}

func (r *Sqlserver) Grammar() driver.Grammar {
	return NewGrammar(r.config.Writers()[0].Prefix)
}

func (r *Sqlserver) Pool() database.Pool {
	return database.Pool{
		Readers: r.fullConfigsToConfigs(r.config.Readers()),
		Writers: r.fullConfigsToConfigs(r.config.Writers()),
	}
}

func (r *Sqlserver) Processor() driver.Processor {
	return NewProcessor()
}

func (r *Sqlserver) fullConfigsToConfigs(fullConfigs []contracts.FullConfig) []database.Config {
	configs := make([]database.Config, len(fullConfigs))
	for i, fullConfig := range fullConfigs {
		configs[i] = database.Config{
			Charset:      fullConfig.Charset,
			Connection:   fullConfig.Connection,
			Dsn:          fullConfig.Dsn,
			Database:     fullConfig.Database,
			Dialector:    fullConfigToDialector(fullConfig),
			Driver:       Name,
			Host:         fullConfig.Host,
			NameReplacer: fullConfig.NameReplacer,
			NoLowerCase:  fullConfig.NoLowerCase,
			Password:     fullConfig.Password,
			Port:         fullConfig.Port,
			Prefix:       fullConfig.Prefix,
			Singular:     fullConfig.Singular,
			Username:     fullConfig.Username,
			Timezone:     fullConfig.Timezone,
		}
	}

	return configs
}

func dsn(fullConfig contracts.FullConfig) string {
	if fullConfig.Dsn != "" {
		return fullConfig.Dsn
	}
	if fullConfig.Host == "" {
		return ""
	}

	return fmt.Sprintf("sqlserver://%s:%s@%s:%d?database=%s&charset=%s&timezone=%s&MultipleActiveResultSets=true",
		fullConfig.Username, fullConfig.Password, fullConfig.Host, fullConfig.Port, fullConfig.Database, fullConfig.Charset, fullConfig.Timezone)
}

func fullConfigToDialector(fullConfig contracts.FullConfig) gorm.Dialector {
	dsn := dsn(fullConfig)
	if dsn == "" {
		return nil
	}

	return sqlserver.New(sqlserver.Config{
		DSN: dsn,
	})
}
