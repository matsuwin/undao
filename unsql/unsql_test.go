package unsql

import (
	_ "github.com/mattn/go-sqlite3"
	"github.com/pkg/errors"
	"testing"
)

type Topic struct {
	Title  string `json:"title" xorm:"title"`
	Author string `json:"author" xorm:"author"`
}

var DB = NewDatabase("sqlite3", "/home/master/data/aigent/webserver/assets/database/server.db")

func init() {
	if e := DB.Sync2(
		&Topic{},
	); e != nil {
		panic(errors.Wrap(e, "> DB.Sync2"))
	}
}

func Test(t *testing.T) {

	//obj := Topic{Title: "Jsdfkn", Author: "hui"}
	//if _, e := DB.InsertOne(&obj); e != nil {panic(e)}

	//if _, e := DB.Delete(Topic{Title: "4567890987654"}); e != nil {panic(e)}

	//if e := Update(DB.DB().DB, DB.TableName(Topic{}),
	//	M{"author": "yan"},
	//	M{"title": "445529834578"}); e != nil {panic(e)}

	//has := IDExists(DB, Topic{Title: "25433473"})
	//fmt.Println(has)

	//ret := Topic{}
	//if _, e := DB.Where("author=?", "yan").Get(&ret); e != nil {
	//	panic(e)
	//}
	//stddebug.Json(ret)
}

//
//import "testing"
//
//func Test(t *testing.T) { main() }
//
//var db = NewDatabase("", "", "")
//
//func main() {
//
//	tx, _ := db.Begin()
//	if e := Add(tx, "", M{}); e != nil {
//		panic(e)
//	}
//	if e := tx.Commit(); e != nil {
//		panic(e)
//	}
//}
