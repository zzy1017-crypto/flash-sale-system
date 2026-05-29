package middleware

import (
	"flash-sale-system/internal/cache"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
)

func RateLimitMiddleware(rdb *cache.RedisClient, limit int) gin.HandlerFunc { //RateLimitMiddleware 函数返回一个 Gin 中间件函数，用于实现基于 Redis 的简单速率限制机制

	return func(c *gin.Context) {

		key := "rate_limit:" + c.FullPath() //每个接口独立限流key

		count, err := rdb.Client.Incr(cache.Ctx, key).Result() //Redis原子操作，key=key+1,原子自增计数
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{
				"error": "redis error",
			})
			c.Abort()
			return
		}

		if count == 1 { //第一次访问，设置过期时间为1秒
			err := rdb.Client.Expire(cache.Ctx, key, time.Second).Err()
			if err != nil {
				c.JSON(http.StatusInternalServerError, gin.H{
					"error": "expire error",
				})
				c.Abort()
				return
			}
		}

		if count > int64(limit) { //如果请求数超过限制，返回 429 Too Many Requests 错误
			c.JSON(http.StatusTooManyRequests, gin.H{
				"error": "too many requests",
			})
			c.Abort()
			return
		}

		c.Next() //放行请求，继续处理后续的 handler
	}
}
