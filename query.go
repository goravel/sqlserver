package sqlserver

import "gorm.io/gorm/clause"

type Query struct {
}

func NewQuery() *Query {
	return &Query{}
}

func (r *Query) LockForUpdate() clause.Expression {
	return With("rowlock", "updlock", "holdlock")
}

func (r *Query) RandomOrder() string {
	return "NEWID()"
}

func (r *Query) SharedLock() clause.Expression {
	return With("rowlock", "holdlock")
}
