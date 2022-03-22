package unmql

import "go.mongodb.org/mongo-driver/bson"

// Rename 字段重命名
func (*umt) Rename(v string) bson.M { return bson.M{"$rename": v} }

// Set 字段值更新
func (*umt) Set(v interface{}) bson.M { return bson.M{"$set": v} }

// Unset 删除字段
func (*umt) Unset(v interface{}) bson.M { return bson.M{"$unset": v} }

// Inc 数值字段增减
func (*umt) Inc(v interface{}) bson.M { return bson.M{"$inc": v} }

// Mul 数值字段乘积
func (*umt) Mul(v interface{}) bson.M { return bson.M{"$mul": v} }

// Min 指定的值小于原本值就更新
func (*umt) Min(v interface{}) bson.M { return bson.M{"$min": v} }

// Max 指定的值大于原本值就更新
func (*umt) Max(v interface{}) bson.M { return bson.M{"$max": v} }

// ArrayAddToSet 数组字段为空就更新
func (*umt) ArrayAddToSet(v interface{}) bson.M { return bson.M{"$addToSet": v} }

// ArrayPull 删除数组中指定值
func (*umt) ArrayPull(v interface{}) bson.M { return bson.M{"$pull": v} }

// ArrayPullAll 删除数组中多个指定值
func (*umt) ArrayPullAll(v interface{}) bson.M { return bson.M{"$pullAll": v} }

// ArrayPopHead 删除数组头
func (*umt) ArrayPopHead(array string) bson.M { return bson.M{"$pop": bson.M{array: -1}} }

// ArrayPopTail 删除数组尾
func (*umt) ArrayPopTail(array string) bson.M { return bson.M{"$pop": bson.M{array: 1}} }

// ArrayAppend 数组内容追加
func (*umt) ArrayAppend(array string, v interface{}) bson.M {
	return bson.M{"$push": bson.M{array: bson.M{"$each": v}}}
}
