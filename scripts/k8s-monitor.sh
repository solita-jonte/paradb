#!/bin/bash

echo "Starting monitoring of ParaDB"

helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
helm repo update

helm install monitoring prometheus-community/kube-prometheus-stack \
  --namespace monitoring \
  --create-namespace

# Provision the Grafana dashboard
kubectl create configmap paradb-grafana-dashboard \
  --from-file=paradb-dashboard.json=scripts/grafana-paradb-dash.json \
  -n monitoring \
  --dry-run=client -o yaml \
  | kubectl apply -f -
kubectl label configmap paradb-grafana-dashboard \
  grafana_dashboard="1" -n monitoring --overwrite

kubectl -n monitoring wait \
  --for=condition=Ready pod \
  --selector=app.kubernetes.io/name=grafana \
  --timeout=300s

echo "Grafana user is admin, the password is:"
kubectl get secret -n monitoring monitoring-grafana \
  -o jsonpath="{.data.admin-password}" | base64 --decode
echo

kubectl -n monitoring port-forward svc/monitoring-grafana 3000:80 &

sleep 2
python -m webbrowser "http://localhost:3000/d/paradb-dash/paradb-dash"
