package mq

import (
	"encoding/json"
	"flash-sale-system/internal/service"
)

func (mq *RabbitMQ) StartConsumer(
	orderService *service.OrderService,
) error {
	//开始监听 "order_queue" 队列，获取消息通道 msgs
	msgs, err := mq.Channel.Consume(
		"order_queue",
		"",
		true,
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

			var orderMsg OrderMessage

			err := json.Unmarshal(
				msg.Body,
				&orderMsg,
			) //JSON反序列化，将消息体转换为 OrderMessage 结构体

			if err != nil {
				continue
			}
			//调用订单服务的 CreateOrder 方法创建订单，传入消息中的用户ID和商品ID，忽略返回的订单对象和错误
			_, _ = orderService.CreateOrder(
				orderMsg.UserID,
				orderMsg.ProductID,
			)
		}
	}()

	return nil
}
