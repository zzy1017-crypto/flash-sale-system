package service

import (
	"context"
	"flash-sale-system/internal/cache"
	"fmt"

	"github.com/redis/go-redis/v9"
)

type StockService struct {
	client *redis.Client
}

func NewStockService(rdb *cache.RedisClient) *StockService {
	return &StockService{
		client: rdb.Client,
	}
}

var deductStockScript = redis.NewScript(`
local stock = tonumber(redis.call("get", KEYS[1]))     --获取当前库存数量，并转换为数字类型,local定义局部变量,tonumber将字符串转为数字
if stock == nil then
   return -1
end                                                --如果库存不存在，返回 -1 表示错误

if stock <= 0 then
return  0
end                                                 --如果库存不足，返回 0 表示扣减失败

redis.call("decr", KEYS[1])            --库存足够，执行扣减操作，使用 Redis 的 DECR 命令将库存数量减 1,decr命令会自动将字符串类型的库存数量转换为数字类型,并进行原子操作,确保在高并发环境下不会出现竞争条件
return 1                                    --扣减成功，返回 1 表示成功  
`)

func (s *StockService) DeductStock(key string) (int64, error) {
	res, err := deductStockScript.Run(
		context.Background(),
		s.client,
		[]string{key},
	).Result()

	if err != nil {
		return 0, err
	}

	result, ok := res.(int64)
	if !ok {
		return 0, fmt.Errorf("invalid result type: %T", res)
	} //将 Lua 脚本的执行结果转换为 int64 类型，以便在 Go 代码中使用

	return result, nil
}

func (s *StockService) RollbackStock(key string) error {

	return s.client.Incr(
		context.Background(),
		key,
	).Err()
}
