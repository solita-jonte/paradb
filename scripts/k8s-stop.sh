#!/bin/bash
kubectl delete -f k8s/ --ignore-not-found
kubectl delete namespace monitoring --ignore-not-found
kubectl delete namespace logging --ignore-not-found
