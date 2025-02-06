package sqlserver

import (
	"fmt"
	"slices"
	"strings"

	"github.com/spf13/cast"

	contractsschema "github.com/goravel/framework/contracts/database/schema"
	"github.com/goravel/framework/database/schema"
	"github.com/goravel/framework/errors"
)

var _ contractsschema.Grammar = &Grammar{}

type Grammar struct {
	attributeCommands []string
	modifiers         []func(contractsschema.Blueprint, contractsschema.ColumnDefinition) string
	prefix            string
	serials           []string
	wrap              *Wrap
}

func NewGrammar(prefix string) *Grammar {
	grammar := &Grammar{
		attributeCommands: []string{schema.CommandComment, schema.CommandDefault},
		prefix:            prefix,
		serials:           []string{"bigInteger", "integer", "mediumInteger", "smallInteger", "tinyInteger"},
		wrap:              NewWrap(prefix),
	}
	grammar.modifiers = []func(contractsschema.Blueprint, contractsschema.ColumnDefinition) string{
		grammar.ModifyDefault,
		grammar.ModifyIncrement,
		grammar.ModifyNullable,
	}

	return grammar
}

func (r *Grammar) CompileAdd(blueprint contractsschema.Blueprint, command *contractsschema.Command) string {
	return fmt.Sprintf("alter table %s add %s", r.wrap.Table(blueprint.GetTableName()), r.getColumn(blueprint, command.Column))
}

func (r *Grammar) CompileChange(blueprint contractsschema.Blueprint, command *contractsschema.Command) []string {
	return []string{
		r.CompileDropDefaultConstraint(blueprint, command),
		fmt.Sprintf("alter table %s alter column %s", r.wrap.Table(blueprint.GetTableName()), r.getColumn(blueprint, command.Column)),
	}
}

func (r *Grammar) CompileColumns(_, table string) (string, error) {
	schema, table, err := parseSchemaAndTable(table, "")
	if err != nil {
		return "", err
	}

	table = r.prefix + table

	newSchema := "schema_name()"
	if schema != "" {
		newSchema = r.wrap.Quote(schema)
	}

	return fmt.Sprintf(
		"select col.name, type.name as type_name, "+
			"col.max_length as length, col.precision as precision, col.scale as places, "+
			"col.is_nullable as nullable, def.definition as [default], "+
			"col.is_identity as autoincrement, col.collation_name as collation, "+
			"com.definition as [expression], is_persisted as [persisted], "+
			"cast(prop.value as nvarchar(max)) as comment "+
			"from sys.columns as col "+
			"join sys.types as type on col.user_type_id = type.user_type_id "+
			"join sys.objects as obj on col.object_id = obj.object_id "+
			"join sys.schemas as scm on obj.schema_id = scm.schema_id "+
			"left join sys.default_constraints def on col.default_object_id = def.object_id and col.object_id = def.parent_object_id "+
			"left join sys.extended_properties as prop on obj.object_id = prop.major_id and col.column_id = prop.minor_id and prop.name = 'MS_Description' "+
			"left join sys.computed_columns as com on col.column_id = com.column_id and col.object_id = com.object_id "+
			"where obj.type in ('U', 'V') and obj.name = %s and scm.name = %s "+
			"order by col.column_id", r.wrap.Quote(table), newSchema), nil
}

func (r *Grammar) CompileComment(_ contractsschema.Blueprint, _ *contractsschema.Command) string {
	return ""
}

func (r *Grammar) CompileCreate(blueprint contractsschema.Blueprint) string {
	return fmt.Sprintf("create table %s (%s)", r.wrap.Table(blueprint.GetTableName()), strings.Join(r.getColumns(blueprint), ", "))
}

func (r *Grammar) CompileDefault(blueprint contractsschema.Blueprint, command *contractsschema.Command) string {
	if command.Column.IsChange() && command.Column.GetDefault() != nil {
		return fmt.Sprintf("alter table %s add default %s for %s",
			r.wrap.Table(blueprint.GetTableName()),
			schema.ColumnDefaultValue(command.Column.GetDefault()),
			r.wrap.Column(command.Column.GetName()),
		)
	}

	return ""
}

func (r *Grammar) CompileDrop(blueprint contractsschema.Blueprint) string {
	return fmt.Sprintf("drop table %s", r.wrap.Table(blueprint.GetTableName()))
}

func (r *Grammar) CompileDropAllDomains(_ []string) string {
	return ""
}

func (r *Grammar) CompileDropAllForeignKeys() string {
	return `DECLARE @sql NVARCHAR(MAX) = N'';
            SELECT @sql += 'ALTER TABLE '
                + QUOTENAME(OBJECT_SCHEMA_NAME(parent_object_id)) + '.' + + QUOTENAME(OBJECT_NAME(parent_object_id))
                + ' DROP CONSTRAINT ' + QUOTENAME(name) + ';'
            FROM sys.foreign_keys;

            EXEC sp_executesql @sql;`
}

func (r *Grammar) CompileDropAllTables(_ string, _ []contractsschema.Table) []string {
	return []string{
		r.CompileDropAllForeignKeys(),
		"EXEC sp_msforeachtable 'DROP TABLE ?'",
	}
}

func (r *Grammar) CompileDropAllTypes(_ string, _ []contractsschema.Type) []string {
	return nil
}

func (r *Grammar) CompileDropAllViews(_ string, _ []contractsschema.View) []string {
	return []string{`
DECLARE @sql NVARCHAR(MAX) = N'';
SELECT @sql += 'DROP VIEW ' + QUOTENAME(OBJECT_SCHEMA_NAME(object_id)) + '.' + QUOTENAME(name) + ';' FROM sys.views;
EXEC sp_executesql @sql;`,
	}
}

func (r *Grammar) CompileDropColumn(blueprint contractsschema.Blueprint, command *contractsschema.Command) []string {
	columns := r.wrap.Columns(command.Columns)

	dropExistingConstraintsSql := r.CompileDropDefaultConstraint(blueprint, command)

	return []string{
		fmt.Sprintf("%s alter table %s drop column %s", dropExistingConstraintsSql, r.wrap.Table(blueprint.GetTableName()), strings.Join(columns, ", ")),
	}
}

func (r *Grammar) CompileDropDefaultConstraint(blueprint contractsschema.Blueprint, command *contractsschema.Command) string {
	// TODO Add change logic
	columns := fmt.Sprintf("'%s'", strings.Join(command.Columns, "','"))
	if command.Column != nil && command.Column.IsChange() {
		columns = fmt.Sprintf("'%s'", command.Column.GetName())
	}

	table := r.wrap.Table(blueprint.GetTableName())
	tableName := r.wrap.Quote(table)

	return fmt.Sprintf("DECLARE @sql NVARCHAR(MAX) = '';"+
		"SELECT @sql += 'ALTER TABLE %s DROP CONSTRAINT ' + OBJECT_NAME([default_object_id]) + ';' "+
		"FROM sys.columns "+
		"WHERE [object_id] = OBJECT_ID(%s) AND [name] in (%s) AND [default_object_id] <> 0;"+
		"EXEC(@sql);", table, tableName, columns)
}

func (r *Grammar) CompileDropForeign(blueprint contractsschema.Blueprint, command *contractsschema.Command) string {
	return fmt.Sprintf("alter table %s drop constraint %s", r.wrap.Table(blueprint.GetTableName()), r.wrap.Column(command.Index))
}

func (r *Grammar) CompileDropFullText(_ contractsschema.Blueprint, _ *contractsschema.Command) string {
	return ""
}

func (r *Grammar) CompileDropIfExists(blueprint contractsschema.Blueprint) string {
	table := r.wrap.Table(blueprint.GetTableName())

	return fmt.Sprintf("if object_id(%s, 'U') is not null drop table %s", r.wrap.Quote(table), table)
}

func (r *Grammar) CompileDropIndex(blueprint contractsschema.Blueprint, command *contractsschema.Command) string {
	return fmt.Sprintf("drop index %s on %s", r.wrap.Column(command.Index), r.wrap.Table(blueprint.GetTableName()))
}

func (r *Grammar) CompileDropPrimary(blueprint contractsschema.Blueprint, command *contractsschema.Command) string {
	return fmt.Sprintf("alter table %s drop constraint %s", r.wrap.Table(blueprint.GetTableName()), r.wrap.Column(command.Index))
}

func (r *Grammar) CompileDropUnique(blueprint contractsschema.Blueprint, command *contractsschema.Command) string {
	return r.CompileDropIndex(blueprint, command)
}

func (r *Grammar) CompileForeign(blueprint contractsschema.Blueprint, command *contractsschema.Command) string {
	sql := fmt.Sprintf("alter table %s add constraint %s foreign key (%s) references %s (%s)",
		r.wrap.Table(blueprint.GetTableName()),
		r.wrap.Column(command.Index),
		r.wrap.Columnize(command.Columns),
		r.wrap.Table(command.On),
		r.wrap.Columnize(command.References))
	if command.OnDelete != "" {
		sql += " on delete " + command.OnDelete
	}
	if command.OnUpdate != "" {
		sql += " on update " + command.OnUpdate
	}

	return sql
}

func (r *Grammar) CompileForeignKeys(schema, table string) string {
	newSchema := "schema_name()"
	if schema != "" {
		newSchema = r.wrap.Quote(schema)
	}

	return fmt.Sprintf(
		`SELECT 
			fk.name AS name, 
			string_agg(lc.name, ',') WITHIN GROUP (ORDER BY fkc.constraint_column_id) AS columns, 
			fs.name AS foreign_schema, 
			ft.name AS foreign_table, 
			string_agg(fc.name, ',') WITHIN GROUP (ORDER BY fkc.constraint_column_id) AS foreign_columns, 
			fk.update_referential_action_desc AS on_update, 
			fk.delete_referential_action_desc AS on_delete 
		FROM sys.foreign_keys AS fk 
		JOIN sys.foreign_key_columns AS fkc ON fkc.constraint_object_id = fk.object_id 
		JOIN sys.tables AS lt ON lt.object_id = fk.parent_object_id 
		JOIN sys.schemas AS ls ON lt.schema_id = ls.schema_id 
		JOIN sys.columns AS lc ON fkc.parent_object_id = lc.object_id AND fkc.parent_column_id = lc.column_id 
		JOIN sys.tables AS ft ON ft.object_id = fk.referenced_object_id 
		JOIN sys.schemas AS fs ON ft.schema_id = fs.schema_id 
		JOIN sys.columns AS fc ON fkc.referenced_object_id = fc.object_id AND fkc.referenced_column_id = fc.column_id 
		WHERE lt.name = %s AND ls.name = %s 
		GROUP BY fk.name, fs.name, ft.name, fk.update_referential_action_desc, fk.delete_referential_action_desc`,
		r.wrap.Quote(table),
		newSchema,
	)
}

func (r *Grammar) CompileFullText(_ contractsschema.Blueprint, _ *contractsschema.Command) string {
	return ""
}

func (r *Grammar) CompileIndex(blueprint contractsschema.Blueprint, command *contractsschema.Command) string {
	return fmt.Sprintf("create index %s on %s (%s)",
		r.wrap.Column(command.Index),
		r.wrap.Table(blueprint.GetTableName()),
		r.wrap.Columnize(command.Columns),
	)
}

func (r *Grammar) CompileIndexes(_, table string) (string, error) {
	schema, table, err := parseSchemaAndTable(table, "")
	if err != nil {
		return "", err
	}

	table = r.prefix + table

	newSchema := "schema_name()"
	if schema != "" {
		newSchema = r.wrap.Quote(schema)
	}

	return fmt.Sprintf(
		"select idx.name as name, string_agg(col.name, ',') within group (order by idxcol.key_ordinal) as columns, "+
			"idx.type_desc as [type], idx.is_unique as [unique], idx.is_primary_key as [primary] "+
			"from sys.indexes as idx "+
			"join sys.tables as tbl on idx.object_id = tbl.object_id "+
			"join sys.schemas as scm on tbl.schema_id = scm.schema_id "+
			"join sys.index_columns as idxcol on idx.object_id = idxcol.object_id and idx.index_id = idxcol.index_id "+
			"join sys.columns as col on idxcol.object_id = col.object_id and idxcol.column_id = col.column_id "+
			"where tbl.name = %s and scm.name = %s "+
			"group by idx.name, idx.type_desc, idx.is_unique, idx.is_primary_key",
		r.wrap.Quote(table),
		newSchema,
	), nil
}

func (r *Grammar) CompilePrimary(blueprint contractsschema.Blueprint, command *contractsschema.Command) string {
	return fmt.Sprintf("alter table %s add constraint %s primary key (%s)",
		r.wrap.Table(blueprint.GetTableName()),
		r.wrap.Column(command.Index),
		r.wrap.Columnize(command.Columns))
}

func (r *Grammar) CompileRename(blueprint contractsschema.Blueprint, command *contractsschema.Command) string {
	return fmt.Sprintf("sp_rename %s, %s", r.wrap.Quote(r.wrap.Table(blueprint.GetTableName())), r.wrap.Table(command.To))
}

func (r *Grammar) CompileRenameColumn(_ contractsschema.Schema, blueprint contractsschema.Blueprint, command *contractsschema.Command) string {
	return fmt.Sprintf("sp_rename %s, %s, N'COLUMN'",
		r.wrap.Quote(r.wrap.Table(blueprint.GetTableName())+"."+r.wrap.Column(command.From)),
		r.wrap.Column(command.To),
	)
}

func (r *Grammar) CompileRenameIndex(_ contractsschema.Schema, blueprint contractsschema.Blueprint, command *contractsschema.Command) []string {
	return []string{
		fmt.Sprintf("sp_rename %s, %s, N'INDEX'", r.wrap.Quote(r.wrap.Table(blueprint.GetTableName())+"."+r.wrap.Column(command.From)), r.wrap.Column(command.To)),
	}
}

func (r *Grammar) CompileTables(_ string) string {
	return "select t.name as name, schema_name(t.schema_id) as [schema], sum(u.total_pages) * 8 * 1024 as size " +
		"from sys.tables as t " +
		"join sys.partitions as p on p.object_id = t.object_id " +
		"join sys.allocation_units as u on u.container_id = p.hobt_id " +
		"group by t.name, t.schema_id " +
		"order by t.name"
}

func (r *Grammar) CompileTypes() string {
	return ""
}

func (r *Grammar) CompileUnique(blueprint contractsschema.Blueprint, command *contractsschema.Command) string {
	return fmt.Sprintf("create unique index %s on %s (%s)",
		r.wrap.Column(command.Index),
		r.wrap.Table(blueprint.GetTableName()),
		r.wrap.Columnize(command.Columns))
}

func (r *Grammar) CompileViews(_ string) string {
	return "select name, schema_name(v.schema_id) as [schema], definition from sys.views as v " +
		"inner join sys.sql_modules as m on v.object_id = m.object_id " +
		"order by name"
}

func (r *Grammar) GetAttributeCommands() []string {
	return r.attributeCommands
}

func (r *Grammar) ModifyDefault(_ contractsschema.Blueprint, column contractsschema.ColumnDefinition) string {
	if !column.IsChange() && column.GetDefault() != nil {
		return fmt.Sprintf(" default %s", schema.ColumnDefaultValue(column.GetDefault()))
	}

	return ""
}

func (r *Grammar) ModifyNullable(_ contractsschema.Blueprint, column contractsschema.ColumnDefinition) string {
	if column.GetNullable() {
		return " null"
	}

	return " not null"
}

func (r *Grammar) ModifyIncrement(blueprint contractsschema.Blueprint, column contractsschema.ColumnDefinition) string {
	if !column.IsChange() && slices.Contains(r.serials, column.GetType()) && column.GetAutoIncrement() {
		if blueprint.HasCommand("primary") {
			return " identity"
		}
		return " identity primary key"
	}

	return ""
}

func (r *Grammar) TypeBigInteger(_ contractsschema.ColumnDefinition) string {
	return "bigint"
}

func (r *Grammar) TypeBoolean(_ contractsschema.ColumnDefinition) string {
	return "bit"
}

func (r *Grammar) TypeChar(column contractsschema.ColumnDefinition) string {
	return fmt.Sprintf("nchar(%d)", column.GetLength())
}

func (r *Grammar) TypeDate(_ contractsschema.ColumnDefinition) string {
	return "date"
}

func (r *Grammar) TypeDateTime(column contractsschema.ColumnDefinition) string {
	return r.TypeTimestamp(column)
}

func (r *Grammar) TypeDateTimeTz(column contractsschema.ColumnDefinition) string {
	return r.TypeTimestampTz(column)
}

func (r *Grammar) TypeDecimal(column contractsschema.ColumnDefinition) string {
	return fmt.Sprintf("decimal(%d, %d)", column.GetTotal(), column.GetPlaces())
}

func (r *Grammar) TypeDouble(_ contractsschema.ColumnDefinition) string {
	return "double precision"
}

func (r *Grammar) TypeEnum(column contractsschema.ColumnDefinition) string {
	return fmt.Sprintf(`nvarchar(255) check ("%s" in (%s))`, column.GetName(), strings.Join(r.wrap.Quotes(cast.ToStringSlice(column.GetAllowed())), ", "))
}

func (r *Grammar) TypeFloat(column contractsschema.ColumnDefinition) string {
	precision := column.GetPrecision()
	if precision > 0 {
		return fmt.Sprintf("float(%d)", precision)
	}

	return "float"
}

func (r *Grammar) TypeInteger(_ contractsschema.ColumnDefinition) string {
	return "int"
}

func (r *Grammar) TypeJson(_ contractsschema.ColumnDefinition) string {
	return "nvarchar(max)"
}

func (r *Grammar) TypeJsonb(_ contractsschema.ColumnDefinition) string {
	return "nvarchar(max)"
}

func (r *Grammar) TypeLongText(_ contractsschema.ColumnDefinition) string {
	return "nvarchar(max)"
}

func (r *Grammar) TypeMediumInteger(_ contractsschema.ColumnDefinition) string {
	return "int"
}

func (r *Grammar) TypeMediumText(_ contractsschema.ColumnDefinition) string {
	return "nvarchar(max)"
}

func (r *Grammar) TypeSmallInteger(_ contractsschema.ColumnDefinition) string {
	return "smallint"
}

func (r *Grammar) TypeString(column contractsschema.ColumnDefinition) string {
	length := column.GetLength()
	if length > 0 {
		return fmt.Sprintf("nvarchar(%d)", length)
	}

	return "nvarchar(255)"
}

func (r *Grammar) TypeText(_ contractsschema.ColumnDefinition) string {
	return "nvarchar(max)"
}

func (r *Grammar) TypeTime(column contractsschema.ColumnDefinition) string {
	if column.GetPrecision() > 0 {
		return fmt.Sprintf("time(%d)", column.GetPrecision())
	} else {
		return "time"
	}
}

func (r *Grammar) TypeTimeTz(column contractsschema.ColumnDefinition) string {
	return r.TypeTime(column)
}

func (r *Grammar) TypeTimestamp(column contractsschema.ColumnDefinition) string {
	if column.GetUseCurrent() {
		column.Default(schema.Expression("CURRENT_TIMESTAMP"))
	}

	if column.GetPrecision() > 0 {
		return fmt.Sprintf("datetime2(%d)", column.GetPrecision())
	} else {
		return "datetime"
	}
}

func (r *Grammar) TypeTimestampTz(column contractsschema.ColumnDefinition) string {
	if column.GetUseCurrent() {
		column.Default(schema.Expression("CURRENT_TIMESTAMP"))
	}

	if column.GetPrecision() > 0 {
		return fmt.Sprintf("datetimeoffset(%d)", column.GetPrecision())
	} else {
		return "datetimeoffset"
	}
}

func (r *Grammar) TypeTinyInteger(_ contractsschema.ColumnDefinition) string {
	return "tinyint"
}

func (r *Grammar) TypeTinyText(_ contractsschema.ColumnDefinition) string {
	return "nvarchar(255)"
}

func (r *Grammar) getColumns(blueprint contractsschema.Blueprint) []string {
	var columns []string
	for _, column := range blueprint.GetAddedColumns() {
		columns = append(columns, r.getColumn(blueprint, column))
	}

	return columns
}

func (r *Grammar) getColumn(blueprint contractsschema.Blueprint, column contractsschema.ColumnDefinition) string {
	sql := fmt.Sprintf("%s %s", r.wrap.Column(column.GetName()), schema.ColumnType(r, column))

	for _, modifier := range r.modifiers {
		sql += modifier(blueprint, column)
	}

	return sql
}

func parseSchemaAndTable(reference, defaultSchema string) (string, string, error) {
	if reference == "" {
		return "", "", errors.SchemaEmptyReferenceString
	}

	parts := strings.Split(reference, ".")
	if len(parts) > 2 {
		return "", "", errors.SchemaErrorReferenceFormat
	}

	schema := defaultSchema
	if len(parts) == 2 {
		schema = parts[0]
		parts = parts[1:]
	}

	table := parts[0]

	return schema, table, nil
}
