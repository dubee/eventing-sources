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

package kafkaevents

import (
	"context"
	"github.com/Shopify/sarama"
	"github.com/bsm/sarama-cluster"
	"strings"

	"github.com/cloudevents/sdk-go/pkg/cloudevents"
	"github.com/cloudevents/sdk-go/pkg/cloudevents/client"
	"github.com/cloudevents/sdk-go/pkg/cloudevents/types"

	"github.com/knative/pkg/logging"
	"go.uber.org/zap"
)

const (
	eventType = "dev.knative.kafka.event"
)

type Adapter struct {
	Brokers string
	Topic string
	ConsumerGroup string
	SinkURI string
	client client.Client
}

func (a *Adapter) initClient() error {
	if a.client == nil {
		var err error
		if a.client, err = client.NewHTTPClient(
			client.WithTarget(a.SinkURI),
			client.WithHTTPBinaryEncoding(),
			client.WithUUIDs(),
			client.WithTimeNow(),
		); err != nil {
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
	consumer, err := cluster.NewConsumer(strings.Split(a.Brokers, ","), a.ConsumerGroup, []string{a.Topic}, kafkaConfig)
	if err != nil {
		panic(err)
	}
	defer consumer.Close()

	if err := a.initClient(); err != nil {
		logger.Error("Failed to create cloudevent client", zap.Error(err))
		return err
	}

	return a.pollLoop(ctx, consumer, stopCh)
}

func (a *Adapter) pollLoop(ctx context.Context, consumer *cluster.Consumer, stopCh <-chan struct{}) error {
	logger := logging.FromContext(ctx)

	// consume messages, watch signals
	for {
		select {
		case part, ok := <-consumer.Partitions():
			if !ok {
				return nil
			}

			// TODO: needs its own function
			go func(pc cluster.PartitionConsumer) {
				for msg := range pc.Messages() {
					logger.Info("Received %s", msg.Value)
					go a.postMessage(msg.Value, logger)
					consumer.MarkOffset(msg, "")
				}
			}(part)
		case <-stopCh:
			logger.Info("Shutting down...")
			return nil

		}
	}
}

func (a *Adapter) postMessage(data interface{}, logger *zap.SugaredLogger) {

	event := cloudevents.Event{
		Context: cloudevents.EventContextV02{
			Type:   eventType,
			Source: *types.ParseURLRef(a.Topic),
		}.AsV02(),
		Data: data,
	}
	if err := a.client.Send(context.TODO(), event); err != nil {
		logger.Info("sending event to sink failed: '%v'", err)
	}
}
