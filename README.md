# Sqlserver

The Sqlserver driver for facades.Orm() of Goravel.

## Version

| goravel/sqlserver | goravel/framework |
|------------------|-------------------|
| v1.4.*          | v1.16.*           |

## Install

Run the command below in your project to install the package automatically:

```bash
./artisan package:install github.com/goravel/sqlserver
``` 

Or check [the setup file](./setup/setup.go) to install the package manually.

## Schema

If you want to specify a `schema`, you can add the `schema` in the `TableName` function of the model.

```go
func (r *User) TableName() string {
  return "goravel.users"
}
```

## Testing

Run command below to run test:

```bash
go test ./...
```
