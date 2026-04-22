#!/bin/bash
set -e

# Deploy ELK stack (Elasticsearch + Filebeat + Kibana) into the logging namespace.
# Requires: Helm 3, Docker Desktop with Kubernetes enabled.
#
# If this doesn't work, pull the images manually, something like this:
#
# docker pull docker.elastic.co/kibana/kibana:8.5.1  # and so on...
#
# To find out which version is used by your helm chart, use something like this:
#
# kubectl describe pod pre-install-kibana-kibana-abcde -n logging
#
# The same may appy to elasticsearch and filebeat.

echo "Starting logging for ParaDB"

NAMESPACE=logging

# 1. Add the Elastic Helm repo
helm repo add elastic https://helm.elastic.co
helm repo update

# 2. Install Elasticsearch
helm upgrade --install elasticsearch elastic/elasticsearch \
  --namespace "$NAMESPACE" \
  --create-namespace \
  --set replicas=1 \
  --set minimumMasterNodes=1 \
  --set resources.requests.cpu=250m \
  --set resources.requests.memory=1Gi \
  --set resources.limits.cpu=1 \
  --set resources.limits.memory=2Gi \
  --set volumeClaimTemplate.resources.requests.storage=10Gi \
  --set persistence.enabled=true

echo "Waiting for Elasticsearch to become ready..."
kubectl -n "$NAMESPACE" rollout status statefulset/elasticsearch-master --timeout=300s

# 3. Create API key for filebeat to be able to access elasticsearch.
SCRIPT_DIR=$(dirname "${BASH_SOURCE[0]}")
source "$SCRIPT_DIR/k8s-gen-filebeat-api-key.sh"

# 4. Install Filebeat (DaemonSet — collects logs from all nodes/pods)
helm upgrade --install filebeat elastic/filebeat \
  --namespace "$NAMESPACE" \
  --set daemonset.enabled=true \
  --set deployment.enabled=false \
  -f scripts/helm-filebeat.yaml

echo "Waiting for Filebeat to become ready..."
kubectl -n "$NAMESPACE" rollout status daemonset/filebeat-filebeat --timeout=120s

# 5. Install Kibana
helm upgrade --install kibana elastic/kibana \
  --namespace "$NAMESPACE" \
  --set resources.requests.cpu=100m \
  --set resources.requests.memory=512Mi \
  --set resources.limits.cpu=500m \
  --set resources.limits.memory=1Gi \
  --set "elasticsearchHosts=https://elasticsearch-master:9200"

echo "Waiting for Kibana to become ready..."
kubectl -n "$NAMESPACE" rollout status deployment/kibana-kibana --timeout=300s

# 6. Port-forward Kibana and open in browser
kubectl -n "$NAMESPACE" port-forward svc/kibana-kibana 5601:5601 &

sleep 3
python -m webbrowser "http://localhost:5601"
