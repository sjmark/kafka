package kafka

import (
	"bytes"
	"context"
	"encoding/gob"
	"os"
	"sync/atomic"
	"time"

	"github.com/IBM/sarama"
	"github.com/sirupsen/logrus"
)

// 基于kafka的队列实现
type KafkaQueue struct {
	consumer sarama.ConsumerGroup
	producer sarama.SyncProducer
	_log     *logrus.Entry
	topics   []string
	running  int32
}

var _ = Queue((*KafkaQueue)(nil))

// 消费
func NewConsumer(rc *sarama.Config, gid, version string, ips []string) (kq *KafkaQueue, err error) {
	if rc == nil {
		rc = sarama.NewConfig()
		rc.Version, err = sarama.ParseKafkaVersion(version)
		if err != nil {
			rc = nil
			return
		}

		rc.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRange // 分区分配策略
		rc.Consumer.Offsets.Initial = sarama.OffsetOldest                  // 未找到组消费位移的时候从哪边开始消费
	}

	client, err1 := sarama.NewConsumerGroup(ips, gid, rc)
	if err1 != nil {
		err = err1
		return
	}

	kq = &KafkaQueue{consumer: client}
	return
}

// 生产
func NewProducer(rc *sarama.Config, ips []string) (enQueue *KafkaQueue, err error) {
	if rc == nil {
		rc = sarama.NewConfig()
		rc.Producer.RequiredAcks = sarama.WaitForAll
		rc.Producer.Partitioner = sarama.NewRandomPartitioner
		rc.Producer.Return.Successes = true
	}

	client, err1 := sarama.NewSyncProducer(ips, rc)
	if err1 != nil {
		err = err1
		return
	}
	enQueue = &KafkaQueue{producer: client}
	return
}

// 删除topic
func DeleteTopic(rc *sarama.Config, ips, topic []string) (err error) {
	if rc == nil {
		rc = sarama.NewConfig()
		rc.Metadata.Retry.Max = 5
		rc.Metadata.Retry.Backoff = 10 * time.Second
		rc.ClientID = "sarama-prepareTopics"
	}

	client, err1 := sarama.NewClient(ips, rc)
	if err1 != nil {
		err = err1
		return
	}

	bv, err1 := client.Controller()
	if err1 != nil {
		err = err1
		return
	}

	_, err = bv.DeleteTopics(&sarama.DeleteTopicsRequest{Topics: topic, Timeout: time.Minute})
	return
}

func (s *KafkaQueue) SetLogs(log *logrus.Entry) {
	s._log = log
}

func (s *KafkaQueue) SetTopic(topic ...string) {
	s.topics = topic
}

func (s *KafkaQueue) EnQueue(topic string, ts ...Task) (err error) {
	for i := 0; i < len(ts); i++ {
		var network bytes.Buffer                                                                                                  // 标准输入
		err = gob.NewEncoder(&network).Encode(QueueModel{Topic: topic, AllowRetryCount: 3, CreatedTime: time.Now(), Task: ts[i]}) // 编码
		if err != nil {
			return
		}
		_, _, err = s.producer.SendMessage(&sarama.ProducerMessage{Topic: topic, Value: sarama.ByteEncoder(network.Bytes())})
	}
	return
}

func (s *KafkaQueue) EnQueues(tsMap map[string][]Task) (err error) {
	for topic, ts := range tsMap {
		var ps []*sarama.ProducerMessage
		for i := 0; i < len(ts); i++ {
			var network bytes.Buffer                                                                                                  // 标准输入
			err = gob.NewEncoder(&network).Encode(QueueModel{Topic: topic, AllowRetryCount: 3, CreatedTime: time.Now(), Task: ts[i]}) // 编码
			if err != nil {
				return
			}
			ps = append(ps, &sarama.ProducerMessage{Topic: topic, Value: sarama.ByteEncoder(network.Bytes())})
		}

		if len(ps) > 0 {
			err = s.producer.SendMessages(ps)
			if err != nil {
				return
			}
		}
	}
	return
}

func (s *KafkaQueue) IsRunning() bool { return atomic.LoadInt32(&s.running) != 0 }

func (s *KafkaQueue) Quit() {
	// 做相关业务处理或者发送邮件等通知运维
	_ = s.consumer.Close() // 关闭消费对列
	os.Exit(102)           // 只要有错误就直接退出程序并记录日志
}

func (s *KafkaQueue) DeQueue() {
	// 原子操作
	if !atomic.CompareAndSwapInt32(&s.running, 0, 1) {
		return
	}

	var h consumerHandler
	if s.IsRunning() {
		for {
			// 订阅处理消息
			err := s.consumer.Consume(context.Background(), s.topics, &h)
			if err == nil {
				err = h.err
			}

			if err != nil {
				h.err = err
				break
			}
		}
	}
	s._log.Logger.Errorf("KafkaQueue Quit err:%v,topic:%s，qm:%#v", h.err, h.topic, h.qmx.Task)
	s.Quit()
}

type consumerHandler struct {
	qmx   QueueModel
	topic string
	err   error
}

func (*consumerHandler) Setup(_ sarama.ConsumerGroupSession) error   { return nil }
func (*consumerHandler) Cleanup(_ sarama.ConsumerGroupSession) error { return nil }
func (h *consumerHandler) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		h.topic = msg.Topic
		var network bytes.Buffer
		network.Write(msg.Value)
		h.err = gob.NewDecoder(&network).Decode(&h.qmx)
		if h.err != nil {
			return h.err
		}

		h.err = h.qmx.Task.Exec()
		if h.err != nil {
			return h.err
		}

		sess.MarkMessage(msg, "")
		break
	}

	return nil
}
