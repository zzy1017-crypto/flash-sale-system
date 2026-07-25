package mq

import (
	amqp "github.com/rabbitmq/amqp091-go"
)

// 1、2、4秒重试队列的名称和死信队列的名称
const (
	orderRetry1sQueue = "order_retry_1s_queue"
	orderRetry2sQueue = "order_retry_2s_queue"
	orderRetry4sQueue = "order_retry_4s_queue"
)

type RabbitMQ struct {
	Conn    *amqp.Connection //与RabbitMQ服务器的TCP连接
	Channel *amqp.Channel    //在连接上创建的通道，用于发送和接收消息
}

// 创建RabbitMQ实例，连接到RabbitMQ服务器，并声明一个队列
func NewRabbitMQ(url string) (*RabbitMQ, error) {

	//连接到RabbitMQ服务器，返回一个连接对象，如果连接失败则返回错误
	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, err
	}

	//在连接上创建一个通道，返回一个通道对象，如果创建通道失败则返回错误
	ch, err := conn.Channel()
	if err != nil {
		return nil, err
	}

	//声明一个死信队列，确保队列存在，如果声明失败则返回错误
	_, err = ch.QueueDeclare(
		"order_dead_queue",
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return nil, err
	}

	//声明三个重试队列，设置消息的TTL（过期时间）和死信交换机，确保每个重试队列存在，如果声明失败则返回错误
	retryQueues := []struct {
		name string
		ttl  int32
	}{
		{name: orderRetry1sQueue, ttl: 1000},
		{name: orderRetry2sQueue, ttl: 2000},
		{name: orderRetry4sQueue, ttl: 4000},
	}

	for _, retryQueue := range retryQueues {
		_, err = ch.QueueDeclare(
			retryQueue.name,
			true,
			false,
			false,
			false,
			amqp.Table{
				"x-message-ttl":             retryQueue.ttl,
				"x-dead-letter-exchange":    "",
				"x-dead-letter-routing-key": "order_queue",
			},
		)
		if err != nil {
			return nil, err
		}
	}

	//声明一个队列，确保队列存在，如果声明失败则返回错误
	_, err = ch.QueueDeclare(
		"order_queue", //队列名称
		true,          //持久化，确保RabbitMQ重启后队列仍然存在
		false,         //自动删除，当没有消费者时删除队列
		false,         //独占，限制队列只能被当前连接使用
		false,         //no-wait，异步声明队列，不等待服务器响应
		amqp.Table{
			"x-dead-letter-exchange":    "",                 //死信交换机，空字符串表示使用默认交换机
			"x-dead-letter-routing-key": "order_dead_queue", //死信路由键，指定消息发送到死信队列
		}, //其他参数
	)

	if err != nil {
		return nil, err
	}

	//返回一个RabbitMQ对象，包含了连接和通道，可以在程序中其他地方使用这个对象来发送和接收消息
	return &RabbitMQ{
		Conn:    conn,
		Channel: ch,
	}, nil

}
