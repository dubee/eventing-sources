/*
Copyright 2019 The Knative Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package kafka

import (
	"encoding/json"
	"github.com/Shopify/sarama"
	"github.com/bsm/sarama-cluster"
	"github.com/cloudevents/sdk-go/pkg/cloudevents"
	"github.com/cloudevents/sdk-go/pkg/cloudevents/client"
	"github.com/cloudevents/sdk-go/pkg/cloudevents/types"
	"github.com/knative/eventing-sources/pkg/kncloudevents"
	"github.com/knative/pkg/logging"
	"go.uber.org/zap"
	"golang.org/x/net/context"
	"strconv"
	"strings"
)

const (
	eventType = "dev.knative.kafka.event"
)

type AdapterSASL struct {
	Enable   bool
	User     string
	Password string
}

type AdapterTLS struct {
	Enable bool
}

type AdapterNet struct {
	SASL AdapterSASL
	TLS  AdapterTLS
}

type Adapter struct {
	Brokers       string
	Topic         string
	ConsumerGroup string
	Net           AdapterNet
	SinkURI       string
	client        client.Client
}

func (a *Adapter) initClient() error {
	if a.client == nil {
		var err error
		if a.client, err = kncloudevents.NewDefaultClient(a.SinkURI); err != nil {
			return err
		}
	}
	return nil
}

func (a *Adapter) Start(ctx context.Context, stopCh <-chan struct{}) error {
	logger := logging.FromContext(ctx)

	logger.Info("Starting with config: ", zap.Any("adapter", a))

	kafkaConfig := cluster.NewConfig()
	kafkaConfig.Group.Mode = cluster.ConsumerModePartitions
	kafkaConfig.Consumer.Offsets.Initial = sarama.OffsetOldest
	kafkaConfig.Net.SASL.Enable = a.Net.SASL.Enable
	kafkaConfig.Net.SASL.User = a.Net.SASL.User
	kafkaConfig.Net.SASL.Password = a.Net.SASL.Password
	kafkaConfig.Net.TLS.Enable = a.Net.TLS.Enable

	consumer, err := cluster.NewConsumer(strings.Split(a.Brokers, ","), a.ConsumerGroup, []string{a.Topic}, kafkaConfig)
	if err != nil {
		panic(err)
	}
	defer consumer.Close()

	if err := a.initClient(); err != nil {
		logger.Error("Failed to create cloudevent client", zap.Error(err))
		return err
	}

	return a.pollForMessages(ctx, consumer, stopCh)
}

func (a *Adapter) pollForMessages(ctx context.Context, consumer *cluster.Consumer, stopCh <-chan struct{}) error {
	logger := logging.FromContext(ctx)

	for {
		select {
		case partition, ok := <-consumer.Partitions():
			if ok {
				a.consumerMessages(ctx, consumer, partition)
			} else {
				return nil
			}
		case <-stopCh:
			logger.Info("Shutting down...")
			return nil
		}
	}
}

func (a *Adapter) consumerMessages(ctx context.Context, consumer *cluster.Consumer, partitionConsumer cluster.PartitionConsumer) {
	logger := logging.FromContext(ctx)

	for msg := range partitionConsumer.Messages() {
		logger.Info("Received: ", zap.Any("value", string(msg.Value)))

		if err := a.postMessage(ctx, msg); err != nil {
			logger.Info("Error posting message: ", zap.Error(err))
		}

		consumer.MarkOffset(msg, "")
	}
}

func (a *Adapter) postMessage(ctx context.Context, msg *sarama.ConsumerMessage) error {
	logger := logging.FromContext(ctx)

	extensions := map[string]interface{}{
		"key": string(msg.Key),
	}
	event := cloudevents.Event{
		Context: cloudevents.EventContextV02{
			SpecVersion: cloudevents.CloudEventsVersionV02,
			Type:        eventType,
			ID:          "partition:" + strconv.Itoa(int(msg.Partition)) + "/offset:" + strconv.FormatInt(msg.Offset, 10),
			Time:        &types.Timestamp{Time: msg.Timestamp},
			Source:      *types.ParseURLRef(a.Topic),
			ContentType: cloudevents.StringOfApplicationJSON(),
			Extensions:  extensions,
		}.AsV02(),
		Data: a.jsonEncode(ctx, msg.Value),
	}

	if _, err := a.client.Send(ctx, event); err != nil {
		logger.Error("Sending event to sink failed: ", zap.Error(err))
		return err
	} else {
		logger.Info("Successfully sent event to sink")
		return nil
	}
}

func (a *Adapter) jsonEncode(ctx context.Context, value []byte) interface{} {
	var payload map[string]interface{}

	logger := logging.FromContext(ctx)

	if err := json.Unmarshal(value, &payload); err != nil {
		logger.Info("Error unmarshalling JSON: ", zap.Error(err))
		return value
	} else {
		return payload
	}
}
