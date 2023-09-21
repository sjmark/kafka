package kafka

import (
	"fmt"
	"testing"
)

func init() {
	Register(&Hello{})
}

type Hello struct {
	Des string
}

func (s Hello) Exec() error {
	fmt.Println("exec ", s.Des)
	// 如果返回错会自动停掉对列
	return nil
}

var topic = []string{"topic"}
var gid = "dnalims2020"
var host = []string{"192.168.10.47:9092"}

func TestDeleteTopic(t *testing.T) {
	err := DeleteTopic(nil, host, topic)
	if err != nil {
		fmt.Println("====DeleteTopic", err)
		return
	}
}

func TestEnQueue(t *testing.T) {
	kq, err := NewProducer(nil, host)
	if err != nil {
		fmt.Println("====pp", err)
		return
	}
	fmt.Println(kq.EnQueue(topic[0], Hello{Des: "this is test"}))
}

func TestDeQueue(t *testing.T) {
	kq, err := NewConsumer(nil, gid, "2.1.2", host)
	if err != nil {
		fmt.Println("====cc", err)
		return
	}
	kq.SetTopic(topic...)
	kq.DeQueue()
}
