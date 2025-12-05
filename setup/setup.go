package main

import (
	"os"

	"github.com/goravel/framework/packages"
	"github.com/goravel/framework/packages/match"
	"github.com/goravel/framework/packages/modify"
	"github.com/goravel/framework/support/env"
	"github.com/goravel/framework/support/path"
)

func main() {
	config := `map[string]any{
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

	appConfigPath := path.Config("app.go")
	databaseConfigPath := path.Config("database.go")
	modulePath := packages.GetModulePath()
	sqlserverServiceProvider := "&sqlserver.ServiceProvider{}"
	driverContract := "github.com/goravel/framework/contracts/database/driver"
	sqlserverFacades := "github.com/goravel/sqlserver/facades"
	databaseConnectionsConfig := match.Config("database.connections")
	databaseConfig := match.Config("database")

	packages.Setup(os.Args).
		Install(
			// Add sqlserver service provider to app.go if not using bootstrap setup
			modify.When(func(_ map[string]any) bool {
				return !env.IsBootstrapSetup()
			}, modify.GoFile(appConfigPath).
				Find(match.Imports()).Modify(modify.AddImport(modulePath)).
				Find(match.Providers()).Modify(modify.Register(sqlserverServiceProvider))),

			// Add sqlserver service provider to providers.go if using bootstrap setup
			modify.When(func(_ map[string]any) bool {
				return env.IsBootstrapSetup()
			}, modify.AddProviderApply(modulePath, sqlserverServiceProvider)),

			// Add sqlserver connection config to database.go
			modify.GoFile(databaseConfigPath).Find(match.Imports()).Modify(
				modify.AddImport(driverContract),
				modify.AddImport(sqlserverFacades, "sqlserverfacades"),
			).
				Find(databaseConnectionsConfig).Modify(modify.AddConfig("sqlserver", config)).
				Find(databaseConfig).Modify(modify.AddConfig("default", `"sqlserver"`)),
		).
		Uninstall(
			// Remove sqlserver connection config from database.go
			modify.GoFile(databaseConfigPath).
				Find(databaseConfig).Modify(modify.AddConfig("default", `""`)).
				Find(databaseConnectionsConfig).Modify(modify.RemoveConfig("sqlserver")).
				Find(match.Imports()).Modify(
				modify.RemoveImport(driverContract),
				modify.RemoveImport(sqlserverFacades, "sqlserverfacades"),
			),

			// Remove sqlserver service provider from app.go if not using bootstrap setup
			modify.When(func(_ map[string]any) bool {
				return !env.IsBootstrapSetup()
			}, modify.GoFile(appConfigPath).
				Find(match.Providers()).Modify(modify.Unregister(sqlserverServiceProvider)).
				Find(match.Imports()).Modify(modify.RemoveImport(modulePath))),

			// Remove sqlserver service provider from providers.go if using bootstrap setup
			modify.When(func(_ map[string]any) bool {
				return env.IsBootstrapSetup()
			}, modify.RemoveProviderApply(modulePath, sqlserverServiceProvider)),
		).
		Execute()
}
