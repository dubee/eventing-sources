/*
Copyright 2018 The Knative Authors

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

package main

import (
	"flag"
	"log"
	"os"

	"github.com/knative/pkg/signals"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"golang.org/x/net/context"

	"github.com/knative/eventing-sources/pkg/adapter/kafkaevents"
)

const (
	envBrokers = "KAFKA_BROKERS"
	evenTopic = "KAFKA_TOPIC"
	envConsumerGroup = "KAFKA_CONSUMER_GROUP"

	// Sink for messages.
	envSinkURI = "SINK_URI"
)

func getRequiredEnv(envKey string) string {
	val, defined := os.LookupEnv(envKey)
	if !defined {
		log.Fatalf("required environment variable not defined %q", envKey)
	}
	return val
}

func main() {
	flag.Parse()

	ctx := context.Background()
	logCfg := zap.NewProductionConfig()
	logCfg.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	logger, err := logCfg.Build()
	if err != nil {
		log.Fatalf("Unable to create logger: %v", err)
	}

	adapter := &kafkaevents.Adapter{
		Brokers: getRequiredEnv(envBrokers),
		Topic: getRequiredEnv(evenTopic),
		ConsumerGroup: getRequiredEnv(envConsumerGroup),
		SinkURI: getRequiredEnv(envSinkURI),
	}

	logger.Info("Starting Receive Adapter", zap.Reflect("adapter", adapter))

	stopCh := signals.SetupSignalHandler()

	if err := adapter.Start(ctx, stopCh); err != nil {
		logger.Fatal("Failed to start adapter", zap.Error(err))
	}
}
