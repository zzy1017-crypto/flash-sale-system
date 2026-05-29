package service

import (
	"errors"
	"flash-sale-system/internal/database"
	"flash-sale-system/internal/model"
	"fmt"
	"time"

	mysqlDriver "github.com/go-sql-driver/mysql"
	"gorm.io/gorm"
)

var ErrDuplicateOrder = errors.New("duplicate order") //定义一个全局错误变量，表示重复订单错误

type OrderService struct{}

func NewOrderService() *OrderService {
	return &OrderService{}
}

func (s *OrderService) CreateOrder(userID, productID string) (*model.Order, error) {
	var order *model.Order

	err := database.DB.Transaction(func(tx *gorm.DB) error {
		var count int64

		err := tx.Model(&model.Order{}).
			Where("user_id = ? AND product_id = ?", userID, productID).
			Count(&count).Error

		if err != nil {
			return err
		}

		if count > 0 {
			return ErrDuplicateOrder
		} //应用幂等性检查，防止同一用户对同一商品重复下单

		order := &model.Order{
			OrderID:   fmt.Sprintf("order_%d", time.Now().UnixNano()),
			UserID:    userID,
			ProductID: productID,
			Status:    "created",
			CreatedAt: time.Now(),
		}

		err = tx.Create(order).Error
		if err != nil {
			if isDupilicateEntryError(err) {
				return ErrDuplicateOrder
			}
			return err
		}

		return nil

	})

	if err != nil {
		return nil, err
	}

	return order, nil

}

func isDupilicateEntryError(err error) bool {
	var mysqlErr *mysqlDriver.MySQLError

	if errors.As(err, &mysqlErr) {
		return mysqlErr.Number == 1062
	}

	return false
}
