#!/bin/sh
set -eu
until curl -fsS http://elasticsearch:9200/_cluster/health >/dev/null; do sleep 2; done
curl -fsS -X PUT http://elasticsearch:9200/_index_template/uipath-local \
  -H 'Content-Type: application/json' --data-binary @/config/template.json
if ! curl -fsS http://elasticsearch:9200/_data_stream/logs-uipath-default >/dev/null; then
  curl -fsS -X PUT http://elasticsearch:9200/_data_stream/logs-uipath-default
fi
