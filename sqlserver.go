package sqlserver

import (
	"database/sql"
	"fmt"

	sq "github.com/Masterminds/squirrel"
	"github.com/goravel/framework/contracts/config"
	"github.com/goravel/framework/contracts/database"
	"github.com/goravel/framework/contracts/database/driver"
	"github.com/goravel/framework/contracts/log"
	"github.com/goravel/framework/contracts/testing/docker"
	"github.com/goravel/framework/errors"
	"github.com/goravel/sqlserver/contracts"
	"gorm.io/driver/sqlserver"
	"gorm.io/gorm"
)

var _ driver.Driver = &Sqlserver{}

type Sqlserver struct {
	config  contracts.ConfigBuilder
	db      *gorm.DB
	log     log.Log
	version string
}

func NewSqlserver(config config.Config, log log.Log, connection string) *Sqlserver {
	return &Sqlserver{
		config: NewConfig(config, connection),
		log:    log,
	}
}

func (r *Sqlserver) Config() database.Config {
	writers := r.config.Writes()
	if len(writers) == 0 {
		return database.Config{}
	}

	return database.Config{
		Connection:        writers[0].Connection,
		Dsn:               writers[0].Dsn,
		Database:          writers[0].Database,
		Driver:            Name,
		Host:              writers[0].Host,
		Password:          writers[0].Password,
		Port:              writers[0].Port,
		Prefix:            writers[0].Prefix,
		Username:          writers[0].Username,
		Version:           r.getVersion(),
		PlaceholderFormat: sq.AtP,
	}
}

func (r *Sqlserver) DB() (*sql.DB, error) {
	gormDB, _, err := r.Gorm()
	if err != nil {
		return nil, err
	}

	return gormDB.DB()
}

func (r *Sqlserver) Docker() (docker.DatabaseDriver, error) {
	writers := r.config.Writes()
	if len(writers) == 0 {
		return nil, errors.DatabaseConfigNotFound
	}

	return NewDocker(r.config, writers[0].Database, writers[0].Username, writers[0].Password), nil
}

func (r *Sqlserver) Explain(sql string, vars ...any) string {
	return sqlserver.New(sqlserver.Config{}).Explain(sql, vars...)
}

func (r *Sqlserver) Gorm() (*gorm.DB, driver.GormQuery, error) {
	if r.db != nil {
		return r.db, NewQuery(), nil
	}

	db, err := NewGorm(r.config, r.log).Build()
	if err != nil {
		return nil, nil, err
	}

	r.db = db

	return db, NewQuery(), nil
}

func (r *Sqlserver) Grammar() driver.Grammar {
	return NewGrammar(r.config.Writes()[0].Prefix)
}

func (r *Sqlserver) Processor() driver.Processor {
	return NewProcessor()
}

func (r *Sqlserver) getVersion() string {
	if r.version != "" {
		return r.version
	}

	instance, _, err := r.Gorm()
	if err != nil {
		return ""
	}

	var version struct {
		Value string
	}
	if err := instance.Raw("SELECT SERVERPROPERTY('productversion') AS value;").Scan(&version).Error; err != nil {
		r.version = fmt.Sprintf("UNKNOWN: %s", err)
	} else {
		r.version = version.Value
	}

	return r.version
}
