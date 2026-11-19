# The commands that build, deploy and run the system. `make` lists them.
CLUSTER  ?= csc3104
ENV      ?= k3d#        or eks
TAG      ?= $(shell git describe --always --dirty 2>/dev/null || echo dev)
WORKLOAD ?= tiny
ARGS     ?=#            extra replay flags, e.g. ARGS="--speed 0"
BANK     ?= a
KEY      ?= 2

HELMFILE = helmfile -f deploy/helmfile.yaml.gotmpl -e $(ENV) --state-values-set image.tag=$(TAG)
TF       = terraform -chdir=deploy/terraform
EKS      = $(if $(filter eks,$(ENV)),-f deploy/eks-values.yaml)
comma   := ,
space   := $(subst ,, )

help:  ## list the commands
	@grep -E '^[a-z0-9-]+:.*## ' $(MAKEFILE_LIST) | sed -E 's/:.*## /\t/' | column -t -s "$$(printf '\t')"

# ---- develop

check:  ## the repository checks: lint, format, unit tests
	uv sync --locked
	uv run ruff check .
	uv run ruff format --check .
	uv run pytest

proto:  ## regenerate the gRPC code from protocol.proto
	uv run python -m grpc_tools.protoc -Isrc --python_out=src --pyi_out=src --grpc_python_out=src \
	  src/mulechain/node/protocol.proto

# ---- local cluster (needs docker, k3d, kubectl, helm, helmfile)

image:  ## build the one image every service runs
	docker build -q -t mulechain:$(TAG) .

k3d: image  ## create the local cluster and deploy all three banks
	mkdir -p workloads
	k3d cluster create $(CLUSTER) --config deploy/k3d.yaml --wait
	$(MAKE) load deploy

load: image  ## copy the image into the local cluster (k3d has no registry)
	k3d image import -c $(CLUSTER) mulechain:$(TAG)

k3d-down:  ## delete the local cluster
	k3d cluster delete $(CLUSTER)

# ---- any cluster

deploy:  ## install or update everything in the current cluster (ENV=k3d or eks)
	$(HELMFILE) sync
	kubectl wait -A --all --for=condition=Ready --timeout=15m \
	  kafka,clusters.postgresql.cnpg.io,certificates.cert-manager.io
	for bank in a b c; do kubectl rollout status deployment -n bank-$$bank --timeout=5m; done

replay:  ## replay a workload into the banks: make replay WORKLOAD=slice-6h ARGS="--speed 0"
	helm template replay deploy/charts/replay $(EKS) --set image.tag=$(TAG) --set workload=$(WORKLOAD) \
	  $(if $(strip $(ARGS)),--set 'args={$(subst $(space),$(comma),$(strip $(ARGS)))}') \
	  | kubectl replace --force --cascade=foreground -f -
	kubectl logs -n replay -f job/replay --pod-running-timeout=10m
	kubectl wait -n replay --for=condition=complete job/replay --timeout=10s

cases:  ## each bank's cases as "bank tier payments", to compare with `python -m mulechain.evaluate --cases`
	@for bank in a b c; do kubectl exec -n bank-$$bank deploy/case -- python -c \
	  "import json, urllib.request as u; [print('$$bank'.upper(), c['tier'], *c['payments']) for c in json.load(u.urlopen('http://localhost:8000/api/cases'))]"; done

ui:  ## open a bank's analyst page on http://localhost:8000: make ui BANK=b
	kubectl port-forward -n bank-$(BANK) deploy/case 8000

cluster-check:  ## check NetworkPolicies and mTLS on the running cluster (after `make replay`)
	uv run pytest deploy/test_cluster.py -v

key-add:  ## rotation step 1: give a bank a new key version: make key-add BANK=b KEY=2
	kubectl patch secret bank-keys -n bank-$(BANK) --type merge \
	  -p "{\"data\": {\"$(KEY)\": \"$$(head -c 64 /dev/urandom | base64 -w0)\"}}"

key-drop:  ## rotation step 2, five minutes later: retire an old key version: make key-drop BANK=b KEY=1
	kubectl patch secret bank-keys -n bank-$(BANK) --type json -p '[{"op": "remove", "path": "/data/$(KEY)"}]'

rebuild:  ## empty a bank's Valkey; its scorers rebuild it from the Kafka log: make rebuild BANK=a
	kubectl annotate scaledobject scorer -n bank-$(BANK) --overwrite autoscaling.keda.sh/paused-replicas=0
	kubectl wait pod -n bank-$(BANK) -l app=scorer --for=delete --timeout=5m
	kubectl exec -n bank-$(BANK) deploy/privacy-node -- python -c \
	  "import os, valkey; valkey.Valkey.from_url(os.environ['VALKEY_URL']).flushall()"
	kubectl annotate scaledobject scorer -n bank-$(BANK) autoscaling.keda.sh/paused-replicas-

# ---- AWS (needs aws, terraform, zip, and the lab's credentials in ~/.aws/credentials)

lambda:  ## build the ingestor and summary Lambda zips into deploy/terraform/build
	rm -rf deploy/terraform/build
	mkdir -p deploy/terraform/build/ingestor deploy/terraform/build/summary
	uv export -q --locked --no-default-groups --no-emit-project --no-hashes --no-header \
	  | grep '^confluent-kafka==' > deploy/terraform/build/requirements.txt
	uv pip install -q --target deploy/terraform/build/ingestor --python-platform x86_64-manylinux_2_28 \
	  --python-version 3.12 --only-binary :all: -r deploy/terraform/build/requirements.txt
	for app in ingestor summary; do cp -r src/mulechain deploy/terraform/build/$$app/ \
	  && (cd deploy/terraform/build/$$app && find . -exec touch -t 198001020000 {} + \
	  && find . -type f ! -path '*__pycache__*' | LC_ALL=C sort | zip -q -X ../$$app.zip -@); done

eks: lambda  ## create the AWS environment (EKS, ECR, ElastiCache, Lambdas) and deploy to it
	$(TF) init -input=false
	$(TF) apply -auto-approve -input=false
	$(TF) output -raw helm_values > deploy/eks-values.yaml
	aws s3api head-bucket --bucket $$($(TF) output -raw workloads_bucket) 2>/dev/null \
	  || aws s3 mb s3://$$($(TF) output -raw workloads_bucket)
	aws eks update-kubeconfig --region us-east-1 --name csc3104
	aws ecr get-login-password --region us-east-1 \
	  | docker login -u AWS --password-stdin $$($(TF) output -raw image | cut -d/ -f1)
	docker build -q --platform linux/amd64 -t $$($(TF) output -raw image):$(TAG) .
	docker push -q $$($(TF) output -raw image):$(TAG)
	$(MAKE) deploy ENV=eks

eks-down:  ## destroy the AWS environment (bank namespaces first, so their volumes are deleted)
	if aws eks update-kubeconfig --region us-east-1 --name csc3104; then \
	  kubectl delete kafkatopics -A --all; \
	  kubectl delete namespace bank-a bank-b bank-c --ignore-not-found; \
	  kubectl wait --for=delete pv --all --timeout=5m; fi
	$(TF) destroy -auto-approve

.PHONY: help check proto image k3d load k3d-down deploy replay cases ui cluster-check key-add key-drop rebuild lambda eks eks-down
