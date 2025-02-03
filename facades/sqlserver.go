package facades

import (
	"log"

	"github.com/goravel/framework/contracts/database/driver"

	"github.com/goravel/sqlserver"
)

func Sqlserver(connection string) driver.Driver {
	if sqlserver.App == nil {
		log.Fatalln("please register Sqlserver service provider")
		return nil
	}

	instance, err := sqlserver.App.MakeWith(sqlserver.Binding, map[string]any{
		"connection": connection,
	})
	if err != nil {
		log.Fatalln(err)
		return nil
	}

	return instance.(*sqlserver.Sqlserver)
}
