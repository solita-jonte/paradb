#!/bin/bash
set -e

# Fetch Elastic password. Have Elastic generate an API key. Combine it into a string.
# Store the secret.

ES_PASSWORD=$(kubectl exec -n logging elasticsearch-master-0 -c elasticsearch -- printenv \
              | grep ELASTIC_PASSWORD \
              | cut -d= -f2-)

API_RESPONSE=$(kubectl exec -n logging elasticsearch-master-0 -- \
  curl -s -u elastic:$ES_PASSWORD -k \
  -X POST "https://localhost:9200/_security/api_key" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "filebeat-key",
    "role_descriptors": {
      "filebeat_writer": {
        "cluster": ["monitor", "manage_ilm", "manage_index_templates"],
        "index": [
          {
            "names": ["filebeat-*"],
            "privileges": ["create_index", "write", "create", "index", "manage"]
          }
        ]
      }
    }
  }')

API_KEY=$(echo "$API_RESPONSE" | python3 -c "import sys,json; d=json.load(sys.stdin); print(f\"{d['id']}:{d['api_key']}\")")

kubectl delete secret filebeat-es-api-key -n logging --ignore-not-found
kubectl create secret generic filebeat-es-api-key \
  -n logging \
  --from-literal=api_key="$API_KEY"
