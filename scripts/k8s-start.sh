#!/bin/bash
set -e

# Requires: Docker Desktop with Kubernetes enabled
# (Settings -> Kubernetes -> Enable Kubernetes)

echo "Starting ParaDB"

# 1. Verify Docker Desktop K8s is the active context
kubectl config use-context docker-desktop

# 2. Build the container images (Docker Desktop shares its daemon with K8s)
docker build -t paradb-orchestrator:latest -f Dockerfile.orchestrator .
docker build -t paradb-shard:latest        -f Dockerfile.shard .

# 3. Add service monitor stuff
helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
helm repo update
helm upgrade --install monitoring prometheus-community/kube-prometheus-stack \
  --namespace monitoring \
  --create-namespace

# 4. Apply the Kubernetes manifests
kubectl apply -f k8s/

# 5. Wait for pods to become ready
echo "Waiting for pods..."
kubectl -n paradb rollout status deployment/orchestrator --timeout=60s
kubectl -n paradb rollout status deployment/shard --timeout=60s

echo "Shard API available at http://localhost:3357"
