package sqlserver

import (
	"fmt"
	"strings"

	"github.com/goravel/framework/contracts/database/schema"
	"github.com/spf13/cast"
)

var _ schema.Processor = &Processor{}

type Processor struct {
}

func NewProcessor() *Processor {
	return &Processor{}
}

func (r Processor) ProcessColumns(dbColumns []schema.DBColumn) []schema.Column {
	var columns []schema.Column
	for _, dbColumn := range dbColumns {
		columns = append(columns, schema.Column{
			Autoincrement: dbColumn.Autoincrement,
			Collation:     dbColumn.Collation,
			Comment:       dbColumn.Comment,
			Default:       dbColumn.Default,
			Name:          dbColumn.Name,
			Nullable:      cast.ToBool(dbColumn.Nullable),
			Type:          getType(dbColumn),
			TypeName:      dbColumn.TypeName,
		})
	}

	return columns
}

func (r Processor) ProcessForeignKeys(dbForeignKeys []schema.DBForeignKey) []schema.ForeignKey {
	var foreignKeys []schema.ForeignKey
	for _, dbForeignKey := range dbForeignKeys {
		foreignKeys = append(foreignKeys, schema.ForeignKey{
			Name:           dbForeignKey.Name,
			Columns:        strings.Split(dbForeignKey.Columns, ","),
			ForeignSchema:  dbForeignKey.ForeignSchema,
			ForeignTable:   dbForeignKey.ForeignTable,
			ForeignColumns: strings.Split(dbForeignKey.ForeignColumns, ","),
			OnUpdate:       strings.ToLower(strings.ReplaceAll(dbForeignKey.OnUpdate, "_", " ")),
			OnDelete:       strings.ToLower(strings.ReplaceAll(dbForeignKey.OnDelete, "_", " ")),
		})
	}

	return foreignKeys
}

func (r Processor) ProcessIndexes(dbIndexes []schema.DBIndex) []schema.Index {
	var indexes []schema.Index
	for _, dbIndex := range dbIndexes {
		indexes = append(indexes, schema.Index{
			Columns: strings.Split(dbIndex.Columns, ","),
			Name:    strings.ToLower(dbIndex.Name),
			Type:    strings.ToLower(dbIndex.Type),
			Primary: dbIndex.Primary,
			Unique:  dbIndex.Unique,
		})
	}

	return indexes
}

func (r Processor) ProcessTypes(types []schema.Type) []schema.Type {
	return types
}

func getType(dbColumn schema.DBColumn) string {
	var typeName string
	switch dbColumn.TypeName {
	case "binary", "varbinary", "char", "varchar", "nchar", "nvarchar":
		if dbColumn.Length == -1 {
			typeName = dbColumn.TypeName + "(max)"
		} else {
			typeName = fmt.Sprintf("%s(%d)", dbColumn.TypeName, dbColumn.Length)
		}
	case "decimal", "numeric":
		typeName = fmt.Sprintf("%s(%d,%d)", dbColumn.TypeName, dbColumn.Precision, dbColumn.Places)
	case "float", "datetime2", "datetimeoffset", "time":
		typeName = fmt.Sprintf("%s(%d)", dbColumn.TypeName, dbColumn.Precision)
	default:
		typeName = dbColumn.TypeName
	}

	return typeName
}
