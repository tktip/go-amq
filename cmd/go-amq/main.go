package main

import (
	"context"
	"fmt"
	"github.com/Azure/go-amqp"
	"github.com/sirupsen/logrus"
	"github.com/tktip/go-amq/pkg/amq"
	"github.com/tktip/go-amq/pkg/healthcheck"
	"time"
)

func main() {

	//Starting health check
	go healthcheck.StartHealthService()

	brokers := []string{
		"10.68.4.60:6671",
		"10.68.4.60:6672",
		"10.68.4.60:6673",
	}

	logrus.SetLevel(logrus.DebugLevel)
	//myContext, cancel := context.WithCancel(context.Background())
	//defer cancel()
	s, err := amq.NewServer(context.Background(),
		brokers,
		amq.ServerOptionMaxReadRetries(5),
		amq.ServerOptionSleepTime(time.Second*2),
		amq.ServerOptionVerbose(true),
	)

	var callback amq.ListenCallback
	callback = func(queue string, message *amqp.Message, err amq.ReceiverError) {
		if err != nil {
			logrus.Errorf("Error from queue: ", err.Error())
			return
		}
		if message == nil {
			fmt.Println("nil message")
			return
		}
		message.Accept(context.Background())
		b := message.GetData()
		logrus.Info(queue, " ", string(b))
	}
	_, err = s.Subscribe("test.queue.2", callback)

	if err != nil {
		panic(err.Error())
	}

	/*	s.PublishMessage("test.queue.2", time.Second*3, &amqp.Message{
			Header:      &amqp.MessageHeader{
				Durable:       true,
				Priority:      0,
				TTL:           0,
				FirstAcquirer: false,
				DeliveryCount: 0,
			},
		})
	*/s.StayAlive()
}
