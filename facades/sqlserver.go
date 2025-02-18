package facades

import (
	"fmt"

	"github.com/goravel/framework/contracts/database/driver"

	"github.com/goravel/sqlserver"
)

func Sqlserver(connection string) (driver.Driver, error) {
	if sqlserver.App == nil {
		return nil, fmt.Errorf("please register sqlserver service provider")
	}

	instance, err := sqlserver.App.MakeWith(sqlserver.Binding, map[string]any{
		"connection": connection,
	})
	if err != nil {
		return nil, err
	}

	return instance.(*sqlserver.Sqlserver), nil
}
