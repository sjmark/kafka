package kafka

import (
	"encoding/gob"
	"time"
)

func init() {
	Register(&QueueModel{})
}

// 注册对象用于序列化和反序列化
func Register(obj ...interface{}) {
	if len(obj) == 0 {
		return
	}

	for i := 0; i < len(obj); i++ {
		if obj[i] == nil {
			continue
		}
		gob.Register(obj[i])
	}
}

// 队列任务接口
type Task interface {
	Exec() error
}

// 队列任务模型
type QueueModel struct {
	Topic           string
	AllowRetryCount int       // 允许重试计数
	RetryCount      int       // 重试计数
	CreatedTime     time.Time // 消息创建时间
	Extra           string    // 扩展信息
	Task            Task      // 任务
}

// 队列接口
type Queue interface {
	SetTopic(topic ...string)
	EnQueue(topic string, ts ...Task) error
	EnQueues(tsMap map[string][]Task) error
	DeQueue()
	Quit()
	IsRunning() bool
}
