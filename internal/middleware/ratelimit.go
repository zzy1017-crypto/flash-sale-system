package middleware

import (
	"flash-sale-system/internal/cache"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/redis/go-redis/v9"
)

// 定义一个 Lua 脚本，用于实现限流功能，确保在高并发环境下不会出现竞争条件
var rateLimitScript = redis.NewScript(`
local count = redis.call("incr", KEYS[1])  --使用 Redis 的 INCR 命令将键的值加 1,表示当前请求的计数
if count == 1 then  --如果当前请求是第一次访问该键，则设置键的过期时间，确保在指定的时间窗口内进行限流
	redis.call("pexpire", KEYS[1], ARGV[1])  --使用 Redis 的 PEXPIRE 命令设置键的过期时间,ARGV[1] 是传入的时间窗口（毫秒）
end
return count  --返回当前请求的计数，供调用方判断是否超过限流阈值
`)

// IPRateLimitMiddleware 按照客户端 IP 和接口路径进行限流
func IPRateLimitMiddleware(rdb *cache.RedisClient, limit int, window time.Duration) gin.HandlerFunc {
	return func(c *gin.Context) {
		//获取客户端 IP 地址和请求路径，构建限流键，格式为 "rate_limit:ip:{client_ip}:{request_path}"
		key := "rate_limit:ip:" + c.ClientIP() + ":" + requestPath(c)
		//判断是否允许请求，如果超过限流阈值则返回错误响应
		if !allowRequest(c, rdb, key, limit, window) {
			return
		}

		c.Next()
	}
}

// UserRateLimitMiddleware 按照 JWT 中的用户 ID 和接口路径进行限流
func UserRateLimitMiddleware(rdb *cache.RedisClient, limit int, window time.Duration) gin.HandlerFunc {
	return func(c *gin.Context) {
		userIDValue, exists := c.Get("userID")
		userID, ok := userIDValue.(string)
		//如果用户 ID 不存在或者类型不正确，则返回 401 Unauthorized 错误响应
		if !exists || !ok || userID == "" {
			c.JSON(http.StatusUnauthorized, gin.H{
				"error": "no user",
			})
			c.Abort()
			return
		}

		//构建限流键，格式为 "rate_limit:user:{user_id}:{request_path}"
		key := "rate_limit:user:" + userID + ":" + requestPath(c)
		//判断是否允许请求，如果超过限流阈值则返回错误响应
		if !allowRequest(c, rdb, key, limit, window) {
			return
		}

		c.Next()
	}
}

// allowRequest 判断是否允许请求，如果超过限流阈值则返回错误响应
func allowRequest(
	c *gin.Context,
	rdb *cache.RedisClient,
	key string,
	limit int,
	window time.Duration,
) bool {
	//通过lua脚本获取当前请求的计数，如果超过限流阈值则返回错误响应
	count, err := rateLimitScript.Run(
		cache.Ctx,
		rdb.Client,
		[]string{key},
		window.Milliseconds(),
	).Int64()
	//如果执行 Lua 脚本出错，则返回 500 Internal Server Error 错误响应
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": "redis error",
		})
		c.Abort()
		return false
	}

	//如果当前请求的计数超过限流阈值，则返回 429 Too Many Requests 错误响应
	if count > int64(limit) {
		c.JSON(http.StatusTooManyRequests, gin.H{
			"error": "too many requests",
		})
		c.Abort()
		return false
	}

	return true
}

// requestPath 获取请求的完整路径，如果无法获取则返回请求的 URL 路径
func requestPath(c *gin.Context) string {
	path := c.FullPath()
	if path == "" {
		return c.Request.URL.Path
	}

	return path
}
