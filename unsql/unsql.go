////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

package unsql

// use: github.com/didi/gendry
// use: xorm.io/xorm

import (
	"database/sql"
	"fmt"
	"github.com/didi/gendry/builder"
	"github.com/didi/gendry/scanner"
	"github.com/pkg/errors"
	"os"
	"reflect"
	"strings"
	"xorm.io/xorm"
)

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const caseDB = "*sql.DB"
const caseTx = "*sql.Tx"

// NewDatabase 连接数据库
func NewDatabase(driverName string, dataSourceName string) *xorm.Engine {
	ps := strings.Split(dataSourceName, "/")
	_ = os.MkdirAll(strings.Join(ps[:len(ps)-1], "/"), 0777)
	db, err := xorm.NewEngine(driverName, dataSourceName)
	if err != nil {
		fmt.Printf("%+v\n", errors.New(err.Error()))
		os.Exit(-1)
	}
	if err = db.Ping(); err != nil {
		fmt.Printf("%+v\n", errors.New(err.Error()))
		os.Exit(-1)
	}
	return db
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

func IDExists(db *xorm.Engine, bean interface{}) bool {
	total, err := db.Count(bean)
	if err != nil {
		panic(errors.Wrap(err, "> xorm: db.Count"))
	}
	if total != 0 {
		return true
	}
	return false
}

// Update 数据更新操作
func Update(db interface{}, table string, match M, update M) error {
	var stmt *sql.Stmt
	cond, values, err := builder.BuildUpdate(table, match, update)
	if err != nil {
		return errors.New(err.Error())
	}
	switch reflect.TypeOf(db).String() {
	case caseDB:
		stmt, err = db.(*sql.DB).Prepare(cond)
	case caseTx:
		stmt, err = db.(*sql.Tx).Prepare(cond)
	}
	if err != nil {
		return errors.New(err.Error())
	} else {
		defer func() { _ = stmt.Close() }()
	}
	if _, err = stmt.Exec(values...); err != nil {
		return errors.New(err.Error())
	}
	return nil
}

func Select(results interface{}, stmt *sql.Stmt, args ...interface{}) error {
	/* example
	dataList := make([]Book, 0)
	if stmt, err := db.Prepare(sqlText); err != nil {panic(e)} else {
		defer func() { _ = stmt.Close() }()
		if err = unsql.Select(&dataList, stmt); err != nil {panic(e)}
	}
	*/
	rows, err := stmt.Query(args...)
	if err != nil {
		return errors.New(err.Error())
	}
	defer func() { _ = rows.Close() }()
	if err = scanner.Scan(rows, results); err != nil {
		return errors.New(err.Error())
	}
	return nil
}

/*
const (
	BETWEEN    = "BETWEEN"
	NOTBETWEEN = "NOT BETWEEN"
	IN         = "IN"
	NOTIN      = "NOT IN"
	AND        = "AND"
	OR         = "OR"
	ISNULL     = "IS NULL"
	ISNOTNULL  = "IS NOT NULL"
	EQUAL      = "="
	NOTEQUAL   = "!="
	LIKE       = "LIKE"
	JOIN       = "JOIN"
	INNERJOIN  = "INNER JOIN"
	LEFTJOIN   = "LEFT JOIN"
	RIGHTJOIN  = "RIGHT JOIN"
	UNION      = "UNION"
	UNIONALL   = "UNION ALL"
	DESC       = "DESC"
	ASC        = "ASC"
)

*/

type M map[string]interface{}
