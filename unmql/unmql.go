////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

package unmql

// use: go.mongodb.org/mongo-driver/mongo

import (
	"context"
	"fmt"
	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"log"
	"net/url"
	"strings"
	"time"
)

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// 数据写入模式
// 1.  "w=1"          - 发起写操作，数据被写入指定数量的节点才算成功
// 2.  "w=majority"   - 发起写操作，数据被写入大多数节点才算成功
// 3.  "journal=true" - 发起写操作，落地到journal日志文件中才算成功
const writeConcern = "w=1"

// 数据读取模式
// 1.  "available"    - 读取所有可用的数据
// 2.  "local"        - 读取当前分片所有可用的数据
// 3.  "majority"     - 读取在大多数节点上落地的数据
// 4.  "linearizable" - 线性化读取数据
// 5.  "snapshot"     - 读取最近快照中的数据
const readConcern = "readConcern=available"

// 数据读取节点选择
// 1.  "primary"            - 只选择主节点
// 2.  "primaryPreferred"   - 优先择主节点，如果不可用则选择从节点
// 3.  "secondary"          - 只选择从节点
// 4.  "secondaryPreferred" - 优先择从节点，如果不可用则选择主节点
const readPreference = "readPreference=primaryPreferred"

// NewDatabase 连接数据库
func NewDatabase(nodes, name, user, pwd string) *mongo.Database {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	var err error
	var client *mongo.Client
	var urlString string
	if user != "" {
		format := "mongodb://%s:%s@%s/%s?%s&%s&%s"
		urlString = fmt.Sprintf(format, user,
			url.QueryEscape(pwd), // 对密码 Encode
			nodes, name, writeConcern, readPreference, readConcern)
	} else {
		urlString = fmt.Sprintf("mongodb://%s", nodes)
	}
	//fmt.Println(urlString)
	client, err = mongo.Connect(ctx, options.Client().ApplyURI(urlString))
	if err != nil {
		panic(errors.Wrap(err, "> mongo.Connect"))
	}
	err = client.Ping(ctx, readpref.Primary())
	if err != nil {
		err = errors.Wrap(err, "> client.Ping 数据库无响应！")
		log.Fatalf("[  ERROR  ] %s\n", strings.ReplaceAll(err.Error(), "{ Addr:", "\n  { Addr:"))
	}
	return client.Database(name)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// IDExists 检查主键在数据库中是否存在
func IDExists(collection *mongo.Collection, id primitive.ObjectID) bool {
	total, err := collection.CountDocuments(context.Background(), bson.M{"_id": id})
	if err != nil {
		panic(errors.Wrap(err, "> collection.CountDocuments"))
	}
	if total != 0 {
		return true
	}
	return false
}

// Len 数据长度查询
func Len(collection *mongo.Collection, match interface{}) int64 {
	if match == nil {
		match = bson.M{}
	}
	total, err := collection.CountDocuments(context.Background(), match)
	if err != nil {
		panic(errors.Wrap(err, "> collection.CountDocuments"))
	}
	return total
}

// Select 聚合检索
func Select(collection *mongo.Collection, l, p int, match, sort, project interface{}, lookups ...AggregateLookup) (
	func(results interface{}) error, int64, error) {
	/*databind, total, err := unmql.Select(collection, 1, 1, nil, nil, nil)
	  if err != nil {panic(e)}
	  dataList := make([]models, 0, limit)
	  if err = databind(&dataList); err != nil {panic(e)}
	*/

	if l == -1 {
		l = 1<<63 - 1
	}
	if match == nil {
		match = bson.M{}
	}

	// $match
	pipeline := mongo.Pipeline{{{Key: "$match", Value: match}}}

	// $sort
	if sort != nil {
		pipeline = append(pipeline, bson.D{{Key: "$sort", Value: sort}})
	}

	// $limit
	if p != 0 {
		pipeline = append(pipeline, mongo.Pipeline{
			{{Key: "$skip", Value: l * (p - 1)}},
			{{Key: "$limit", Value: l}},
		}...)
	} else {
		pipeline = append(pipeline, bson.D{{Key: "$limit", Value: l}})
	}

	// $project
	if project != nil {
		pipeline = append(pipeline, bson.D{{Key: "$project", Value: project}})
	}

	// $lookup
	appendLookups(&pipeline, lookups)

	ctx := context.Background()
	cur, err := collection.Aggregate(ctx, pipeline)
	if err != nil {
		return nil, 0, errors.Wrap(err, "> collection.Aggregate")
	}

	return func(results interface{}) error {
		defer func() { _ = cur.Close(ctx) }()
		if err = cur.All(ctx, results); err != nil {
			return errors.Wrap(err, "> cur.All")
		}
		return nil
	}, Len(collection, match), nil
}

// DataGroupCount 数据分组计数统计
func DataGroupCount(collection *mongo.Collection, match interface{}, field string) ([]dataGroupCountT, error) {
	/* example
	   dataList, err := unmql.DataGroupCount(dataC.Collection, nil, "name")
	   if err != nil {panic(e)}
	   for i := range dataList {
	   fmt.Printf("%3d) %8d - %s\n", i+1, dataList[i].Count, dataList[i].Field)
	   }
	*/
	if match == nil {
		match = bson.M{}
	}
	result := make([]dataGroupCountT, 0, 100)

	pipeline := mongo.Pipeline{
		{{Key: "$match", Value: match}},
		{{Key: "$group", Value: bson.M{"_id": "$" + field, "count": bson.M{"$sum": 1}}}},
		{{Key: "$sort", Value: bson.M{"count": -1}}},
	}

	ctx := context.Background()
	cur, err := collection.Aggregate(ctx, pipeline)
	if err != nil {
		return nil, errors.Wrap(err, "> collection.Aggregate")
	}
	defer func() { _ = cur.Close(ctx) }()

	if err = cur.All(ctx, &result); err != nil {
		return nil, errors.Wrap(err, "> cur.All")
	}
	return result, nil
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// TimeSeriesQuery 时间序列查询
func TimeSeriesQuery(start, end time.Time) bson.M {
	/* example
	   bson.M{"_id": unmql.TimeSeriesQuery(start, end)}
	*/
	return bson.M{
		"$gt": primitive.NewObjectIDFromTimestamp(start),
		"$lt": primitive.NewObjectIDFromTimestamp(end),
	}
}

// MatchRegex 正则匹配
func MatchRegex(v string) bson.M {
	return bson.M{"$regex": v, "$options": "i"}
}

// Mt 通用认知条件匹配
func Mt(m bson.M) (r bson.M) {
	r = make(bson.M)
	for c, value := range m {
		r[match(c)] = value
	}
	return
}

func match(c string) (r string) {
	switch c {
	default:
		panic(errors.New(" Match Unsupported operation"))
	case "==":
		r = "$eq"
	case "!=":
		r = "$ne"
	case ">":
		r = "$gt"
	case ">=":
		r = "$gte"
	case "<":
		r = "$lt"
	case "<=":
		r = "$lte"
	case "!":
		r = "$not"
	}
	return
}

func appendLookups(pipeline *mongo.Pipeline, lookups []AggregateLookup) {
	for i := range lookups {
		if lookups[i].Limit == 0 {
			lookups[i].Limit = 1
		}

		// $lookup
		pip := mongo.Pipeline{
			{{Key: "$match", Value: bson.M{"$expr": bson.M{"$eq": []string{"$" + lookups[i].ForeignField, "$$id"}}}}},
			{{Key: "$sort", Value: lookups[i].Sort}},
			{{Key: "$limit", Value: lookups[i].Limit}},
			{{Key: "$project", Value: lookups[i].Project}},
		}
		*pipeline = append(*pipeline, bson.D{
			{Key: "$lookup", Value: bson.M{
				"from":     lookups[i].From,
				"let":      bson.M{"id": "$" + lookups[i].LocalField},
				"pipeline": pip,
				"as":       lookups[i].As,
			}}})

		// $unwind
		if lookups[i].Unwind {
			*pipeline = append(*pipeline, bson.D{{Key: "$unwind", Value: bson.M{
				"path": "$" + lookups[i].As,
			}}})
		}
	}
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

type dataGroupCountT struct {
	Field string `bson:"_id" json:"field"`
	Count int    `bson:"count" json:"count"`
}

type AggregateLookup struct {
	From         string
	LocalField   string
	ForeignField string
	As           string
	Sort         bson.M
	Project      bson.M

	Unwind bool
	Limit  int
}

// 更新操作方法集调用句柄
type umt struct{}

var Umt umt
