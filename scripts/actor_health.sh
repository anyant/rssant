#!/bin/bash

curl -X POST -H 'actor-ask-dst: actor.health' http://localhost:6790/api/v1/scheduler | jq
curl -X POST -H 'actor-ask-dst: actor.health' http://localhost:6791/api/v1/harbor/localhost-6791 | jq
curl -X POST -H 'actor-ask-dst: actor.health' http://localhost:6792/api/v1/worker/localhost-6792 | jq
