export KO_DOCKER_REPO="docker.io/dubee"
ko apply -f contrib/kafka/config
kubectl apply -f contrib/kafka/samples/source.yaml
kubectl apply -f contrib/kafka/samples/service.yaml

ko delete -f contrib/kafka/config
kubectl delete -f contrib/kafka/samples/source.yaml
kubectl delete -f contrib/kafka/samples/service.yaml