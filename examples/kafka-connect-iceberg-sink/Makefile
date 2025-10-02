# ========================
# CONFIGURATION
# ========================
CLUSTER     ?= demo-cluster
NAMESPACE   := kafka

# ========================
# TOP-LEVEL TARGETS
# ========================
all: cluster install-namespace build-images install-trino install-storage install-kafka install-catalog
build-images: build-kafka-connect build-producer-image
destroy: delete-namespace delete-cluster

# ========================
# CLUSTER MANAGEMENT
# ========================
.PHONY: cluster
cluster:
	@if kind get clusters | grep -q "^${CLUSTER}$$"; then \
		echo "🐳 Kind cluster '${CLUSTER}' already exists."; \
	else \
		echo "🚀 Creating Kind cluster '${CLUSTER}'..."; \
		kind create cluster --name ${CLUSTER}; \
	fi
	kubectl config use-context kind-${CLUSTER}

delete-cluster:
	kind delete cluster --name ${CLUSTER}


# ========================
# NAMESPACE
# ========================
install-namespace:
	kubectl apply -f namespace/namespace.yaml

delete-namespace:
	kubectl delete -f namespace/namespace.yaml --ignore-not-found
# ========================
# STORAGE
# ========================
install-storage:
	kubectl apply -f storage/minio.yaml
	kubectl apply -f storage/postgres.yaml

# ========================
# PRODUCER
# ========================
build-producer-image:
	docker build -t producer:latest -f docker/producer/Dockerfile .
	kind load docker-image producer:latest --name $(CLUSTER)

run-producer:
	kubectl apply -f producer/producer.yaml -n $(NAMESPACE)

stop-producer:
	kubectl delete -f producer/producer.yaml -n $(NAMESPACE)

# ========================
# KAFKA
# ========================
build-kafka-connect:
	docker build -t kafka-connect-iceberg-sink-connector:0.1 -f docker/kafka-connect/Dockerfile .
	kind load docker-image kafka-connect-iceberg-sink-connector:0.1 --name $(CLUSTER)

install-kafka:
	kubectl create -f 'https://strimzi.io/install/latest?namespace=kafka' -n $(NAMESPACE)
	@echo "⏳ Waiting for Strimzi Operator to be ready..."
	kubectl rollout status deployment/strimzi-cluster-operator -n $(NAMESPACE)
	kubectl apply -f kafka/cluster.yaml
	kubectl apply -f kafka/schema-registry.yaml
	kubectl apply -f kafka/topics.yaml
	@echo "⏳ Waiting for Kafka and Schema Registry to be ready..."
	kubectl rollout status deployment/internal-schema-registry -n $(NAMESPACE)
	@echo "🚀 Deploying schema-registry-init, kafka-connect and kafka ui..."
	kubectl apply -f kafka/schema-registry-init.yaml
	kubectl apply -f kafka/kafka-connect.yaml
	kubectl apply -f kafka/kafka-ui.yaml

# ========================
# LAKEKEEPER
# ========================
install-catalog:
	kubectl apply -f lakekeeper/migrate.yaml
	kubectl apply -f lakekeeper/serve.yaml
	@echo "⏳ Waiting for Lakekeeper serve to be ready..."
	kubectl rollout status deployment/lakekeeper -n catalog
	@echo "🚀 Running Lakekeeper bootstrap..."
	kubectl apply -f lakekeeper/bootstrap.yaml

# ========================
# TRINO
# ========================
install-trino:
	kubectl apply -f trino/kafka-catalog-configmap.yaml
	kubectl apply -f trino/trino-deployment.yaml
