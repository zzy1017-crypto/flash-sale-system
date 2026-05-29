package cache

import (
	"context"

	"github.com/redis/go-redis/v9"
)

var Ctx = context.Background()

type RedisClient struct {
	Client *redis.Client
} //封装一个 Redis 客户端

func NewRedisClient(addr, password string, db int) *RedisClient {
	rdb := redis.NewClient(&redis.Options{ //第三方库 go-redis 提供了一个方便的接口来连接 Redis 服务器
		Addr:     addr,
		Password: password,
		DB:       db,
	})

	_, err := rdb.Ping(Ctx).Result() //使用 Ping 命令测试Redis是否可用
	if err != nil {
		panic("Redis 连接失败:" + err.Error())
	}

	return &RedisClient{
		Client: rdb,
	}
}
