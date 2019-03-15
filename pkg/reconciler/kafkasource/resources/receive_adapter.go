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

package resources

import (
	"fmt"

	"github.com/knative/eventing-sources/pkg/apis/sources/v1alpha1"
	v1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// CloudEventIngressKafka are the arguments needed to create a CloudEvent Kafka. Every
// field is required.
type ReceiveAdapterArgs struct {
	Image   string
	Source  *v1alpha1.KafkaSource
	Labels  map[string]string
	SinkURI string
}

// MakeCloudEventKafka generates (but does not insert into K8s) the CloudEventKafka Deployment
// and Service for CloudEvent Sources.
func MakeReceiveAdapter(args *ReceiveAdapterArgs) *v1.Deployment {
	replicas := int32(1)
	return &v1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:    args.Source.Namespace,
			GenerateName: fmt.Sprintf("kafkaeventsource-%s-", args.Source.Name),
			Labels:       args.Labels,
		},
		Spec: v1.DeploymentSpec{
			Selector: &metav1.LabelSelector{
				MatchLabels: args.Labels,
			},
			Replicas: &replicas,
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: map[string]string{
						"sidecar.istio.io/inject": "true",
					},
					Labels: args.Labels,
				},
				Spec: corev1.PodSpec{
					ServiceAccountName: args.Source.Spec.ServiceAccountName,
					Containers: []corev1.Container{
						{
							Name:  "receive-adapter",
							Image: args.Image,
							Env: []corev1.EnvVar{
								{
									Name:  "KAFKA_BROKERS",
									Value: args.Source.Spec.Brokers,
								},
								{
									Name:  "KAFKA_TOPIC",
									Value: args.Source.Spec.Topic,
								},
								{
									Name:  "KAFKA_CONSUMER_GROUP_ID",
									Value: args.Source.Spec.ConsumerGroupID,
								},
								{
									Name:  "SINK_URI",
									Value: args.SinkURI,
								},
							},
						},
					},
				},
			},
		},
	}
}