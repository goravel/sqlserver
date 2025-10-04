package main

import (
	"os"

	"github.com/goravel/framework/packages"
	"github.com/goravel/framework/packages/match"
	"github.com/goravel/framework/packages/modify"
	"github.com/goravel/framework/support/path"
)

var config = `map[string]any{
        "host":     config.Env("DB_HOST", "127.0.0.1"),
        "port":     config.Env("DB_PORT", 3306),
        "database": config.Env("DB_DATABASE", "forge"),
        "username": config.Env("DB_USERNAME", ""),
        "password": config.Env("DB_PASSWORD", ""),
        "charset":  "utf8mb4",
        "prefix":   "",
        "singular": false,
        "via": func() (driver.Driver, error) {
            return sqlserverfacades.Sqlserver("sqlserver")
        },
    }`

func main() {
	appConfigPath := path.Config("app.go")
	databaseConfigPath := path.Config("database.go")
	modulePath := packages.GetModulePath()
	sqlserverServiceProvider := "&sqlserver.ServiceProvider{}"
	driverContract := "github.com/goravel/framework/contracts/database/driver"
	sqlserverFacades := "github.com/goravel/sqlserver/facades"

	packages.Setup(os.Args).
		Install(
			// Add sqlserver service provider to app.go
			modify.GoFile(appConfigPath).
				Find(match.Imports()).Modify(modify.AddImport(modulePath)).
				Find(match.Providers()).Modify(modify.Register(sqlserverServiceProvider)),

			// Add sqlserver connection config to database.go
			modify.GoFile(databaseConfigPath).Find(match.Imports()).Modify(
				modify.AddImport(driverContract),
				modify.AddImport(sqlserverFacades, "sqlserverfacades"),
			).
				Find(match.Config("database.connections")).Modify(modify.AddConfig("sqlserver", config)),
		).
		Uninstall(
			// Remove sqlserver connection config from database.go
			modify.GoFile(databaseConfigPath).
				Find(match.Config("database.connections")).Modify(modify.RemoveConfig("sqlserver")).
				Find(match.Imports()).Modify(
				modify.RemoveImport(driverContract),
				modify.RemoveImport(sqlserverFacades, "sqlserverfacades"),
			),

			// Remove sqlserver service provider from app.go
			modify.GoFile(path.Config("app.go")).
				Find(match.Providers()).Modify(modify.Unregister(sqlserverServiceProvider)).
				Find(match.Imports()).Modify(modify.RemoveImport(packages.GetModulePath())),
		).
		Execute()
}
