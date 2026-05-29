package model

import "time"

type Order struct {
	ID uint `gorm:"primaryKey"`

	OrderID string `gorm:"type:varchar(64);uniqueIndex;not null"`

	UserID string `gorm:"type:varchar(64);not null;uniqueIndex:idx_user_product"` //添加唯一索引，确保同一用户对同一商品只能有一个订单

	ProductID string `gorm:"type:varchar(64);not null;uniqueIndex:idx_user_product"`

	Status string `gorm:"type:varchar(32);not null;default:created"`

	CreatedAt time.Time
}
