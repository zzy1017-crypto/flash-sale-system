package auth

import (
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
)

// JWT认证中间件，验证请求中的JWT令牌是否合法，并将用户ID存储在上下文中供后续处理使用
func AuthMiddleware() gin.HandlerFunc {

	return func(c *gin.Context) {

		//从请求头中获取Authorization字段，通常格式为"Bearer <token>"
		authHeader := c.GetHeader("Authorization")

		if authHeader == "" {

			c.JSON(
				http.StatusUnauthorized,
				gin.H{
					"error": "no token",
				},
			)

			c.Abort()

			return
		}

		//去掉"Bearer "前缀，获取纯粹的token字符串
		tokenStr := strings.TrimPrefix(
			authHeader,
			"Bearer ",
		)

		//解析token，验证其有效性，并提取claims（载荷）
		claims, err := ParseToken(tokenStr)

		if err != nil {

			c.JSON(
				http.StatusUnauthorized,
				gin.H{
					"error": "invalid token",
				},
			)

			c.Abort()

			return
		}

		//将用户ID存储在上下文中，供后续处理使用
		c.Set("userID", claims.UserID)

		c.Next()
	}
}
