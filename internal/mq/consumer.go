package mq

import (
	"context"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"flash-sale-system/internal/logger"
	"flash-sale-system/internal/service"
	"net"

	mysqlDriver "github.com/go-sql-driver/mysql"
	amqp "github.com/rabbitmq/amqp091-go"
	"go.uber.org/zap"
)

func (mq *RabbitMQ) StartConsumer(
	orderService *service.OrderService,
) error {
	//设置Qos，确保每次只处理一条消息，避免消费者过载
	err := mq.Channel.Qos(
		1,
		0,
		false,
	)
	if err != nil {
		return err
	}

	//开始监听 "order_queue" 队列，获取消息通道 msgs
	msgs, err := mq.Channel.Consume(
		"order_queue",
		"",
		false, //设置手动ack
		false,
		false,
		false,
		nil,
	)

	if err != nil {
		return err
	}

	//启动协程持续消费
	go func() {
		//从消息通道 msgs 中不断接收消息，处理每条订单消息
		for msg := range msgs {

			var orderMsg OrderMessage //定义一个订单消息对象，用于保存从消息中解析出的订单信息，包含用户ID和商品ID等字段

			//将消息体中的 JSON 数据反序列化到订单消息对象中
			err := json.Unmarshal(
				msg.Body,
				&orderMsg,
			)

			//如果反序列化失败则跳过当前消息，放入死信队列，继续处理下一条消息，确保消费者的稳定性和健壮性
			if err != nil {
				rejectMessage(msg, err)
				continue
			}

			//如果订单消息中的用户ID或商品ID为空，则认为消息无效，放入死信队列，继续处理下一条消息
			if orderMsg.UserID == "" || orderMsg.ProductID == "" {
				rejectMessage(msg, errors.New("invalid order message"))
				continue
			}

			//调用订单服务的 CreateOrder 方法创建订单，传入消息中的用户ID和商品ID，忽略返回的订单对象和错误
			_, err = orderService.CreateOrder(
				orderMsg.UserID,
				orderMsg.ProductID,
			)

			//如果创建订单成功或者返回重复订单错误，则认为消息处理成功，手动ack确认消息，继续处理下一条消息
			if err == nil || errors.Is(err, service.ErrDuplicateOrder) {
				ackMessage(msg)
				continue
			}

			//如果发生临时性订单错误（如超时、数据库连接失败等），则将消息重新入队，稍后重试
			if isTransientOrderError(err) {
				nackMessage(msg, err)
				continue
			}

			//如果发生其他错误，则认为消息处理失败，放入死信队列，继续处理下一条消息
			rejectMessage(msg, err)
		}
	}()

	return nil
}

// ackMessage 手动确认消息，确保消息被成功处理
func ackMessage(msg amqp.Delivery) {
	//如果手动确认消息失败，则记录错误日志，包含消息的 delivery tag，帮助排查问题
	if err := msg.Ack(false); err != nil {
		logger.Log.Error(
			"ack order message failed",
			zap.Error(err),
			zap.Uint64("delivery_tag", msg.DeliveryTag),
		)
	}
}

// nackMessage 手动拒绝消息，将消息重新入队，稍后重试
func nackMessage(msg amqp.Delivery, processErr error) {
	//如果重新入队失败，则记录错误日志，包含消息的 delivery tag 和处理错误，帮助排查问题
	if err := msg.Nack(false, true); err != nil {
		logger.Log.Error(
			"nack order message failed",
			zap.Error(err),
			zap.NamedError("process_error", processErr),
			zap.Uint64("delivery_tag", msg.DeliveryTag),
		)
	}
}

// rejectMessage 拒绝消息，将消息放入死信队列
func rejectMessage(msg amqp.Delivery, processErr error) {
	//如果拒绝消息失败，则记录错误日志，包含消息的 delivery tag 和处理错误，帮助排查问题
	if err := msg.Reject(false); err != nil {
		logger.Log.Error(
			"reject order message failed",
			zap.Error(err),
			zap.NamedError("process_error", processErr),
			zap.Uint64("delivery_tag", msg.DeliveryTag),
		)
	}
}

// isTransientOrderError 检查错误是否是临时性订单错误，接受一个错误对象作为参数，返回一个布尔值表示是否是临时性订单错误
func isTransientOrderError(err error) bool {
	//上下文超时错误或数据库连接错误被认为是临时性订单错误，返回 true
	if errors.Is(err, context.DeadlineExceeded) ||
		errors.Is(err, driver.ErrBadConn) {
		return true
	}

	//检查错误是否是网络错误，如果是则认为是临时性订单错误，返回 true
	var netErr net.Error
	if errors.As(err, &netErr) {
		return true
	}

	//检查错误是否是 MySQL 的临时性错误，如果是则认为是临时性订单错误，返回 true
	var mysqlErr *mysqlDriver.MySQLError
	if !errors.As(err, &mysqlErr) {
		return false
	}

	//根据 MySQL 错误代码判断是否是临时性错误，返回 true 或 false
	switch mysqlErr.Number {
	case 1040,
		1042,
		1152,
		1158,
		1159,
		1160,
		1161,
		1205,
		1213,
		2002,
		2003,
		2006,
		2013:
		return true
	default:
		return false
	}
}
