SHELL=/bin/bash -o pipefail

TARGET ?= default
POSTGRES_BASE_VERSION ?= 14

ECR_REGISTRY ?= 011789831835.dkr.ecr.us-east-1.amazonaws.com

GOLANGCI_LINT_VERSION ?= $$(cat .golangci_lint_version)

TEST_ARTIFACT_DIR ?= $(CURDIR)/tmp/test_artifacts
$(TEST_ARTIFACT_DIR):
	mkdir -p $(TEST_ARTIFACT_DIR)

.PHONY: docker_build
# Runs a full multi-platform docker build
docker_build:
	POSTGRES_BASE_VERSION=$(POSTGRES_BASE_VERSION) docker buildx bake --pull $(TARGET)

uname_m := $(shell uname -m)
ifeq ($(uname_m),x86_64)
       PLATFORM ?= linux/amd64
else
       PLATFORM ?= linux/$(uname_m)
endif

.PHONY: docker_build_local
# Runs a docker build for the target platform and loads it into the local docker
# environment
docker_build_local:
	POSTGRES_BASE_VERSION=$(POSTGRES_BASE_VERSION) docker buildx bake --set *.platform=$(PLATFORM) --pull --load $(TARGET)

.PHONY: docker_build_local_postgres
docker_build_local_postgres: TARGET = postgres
# Runs a local docker build for the target platform for postgres and loads it
# into the local docker
docker_build_local_postgres: docker_build_local

.PHONY: docker_build_local_spilo
docker_build_local_spilo: TARGET = spilo
# Runs a local docker build for the target platform for spilo and loads it
# into the local docker
docker_build_local_spilo: docker_build_local

.PHONY: docker_push_local
docker_push_local:
	POSTGRES_BASE_VERSION=$(POSTGRES_BASE_VERSION) docker buildx bake --set *.platform=$(PLATFORM) --set *.tags=$(IMAGE) --pull --push $(TARGET)

POSTGRES_IMAGE ?= ghcr.io/hydradatabase/hydra:dev
.PHONY: docker_push_postgres
docker_push_postgres: TARGET = postgres
docker_push_postgres: IMAGE = $(POSTGRES_IMAGE)
docker_push_postgres: docker_push_local

SPILO_IMAGE ?= $(ECR_REGISTRY)/spilo:dev
.PHONY: docker_push_spilo
docker_push_spilo: TARGET = spilo
docker_push_spilo: IMAGE = $(SPILO_IMAGE)
docker_push_spilo: docker_push_local

.PHONY: docker_check_columnar
docker_check_columnar:
	docker buildx bake --set *.platform=$(PLATFORM) --set columnar.target=checker columnar_13 columnar_14

GO_TEST_FLAGS ?=

.PHONY: acceptance_test
# Runs the acceptance tests
acceptance_test: postgres_acceptance_test spilo_acceptance_test

.PHONY: acceptance_build_test
# Builds local images then runs the acceptance tests
acceptance_build_test: postgres_acceptance_build_test spilo_acceptance_build_test

POSTGRES_IMAGE ?= ghcr.io/hydradatabase/hydra:latest
POSTGRES_UPGRADE_FROM_IMAGE ?= ghcr.io/hydradatabase/hydra:$(POSTGRES_BASE_VERSION)

.PHONY: postgres_acceptance_test
# Runs the postgres acceptance tests
postgres_acceptance_test: $(TEST_ARTIFACT_DIR)
	export ARTIFACT_DIR=$(TEST_ARTIFACT_DIR) && \
		export POSTGRES_IMAGE=$(POSTGRES_IMAGE) && \
		export POSTGRES_UPGRADE_FROM_IMAGE=$(POSTGRES_UPGRADE_FROM_IMAGE) && \
		export EXPECTED_POSTGRES_VERSION=$(POSTGRES_BASE_VERSION) && \
		cd acceptance && \
		go test ./postgres/... $(GO_TEST_FLAGS) -count=1 -v

.PHONY: postgres_pull_upgrade_image
postgres_pull_upgrade_image:
	docker pull $(POSTGRES_UPGRADE_FROM_IMAGE)

.PHONY: postgres_acceptance_build_test
# Builds the postgres image then runs the acceptance tests
postgres_acceptance_build_test: docker_build_local_postgres postgres_pull_upgrade_image postgres_acceptance_test

.PHONY: ecr_login
ecr_login:
	aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin $(ECR_REGISTRY)

SPILO_REPO ?= $(ECR_REGISTRY)/spilo
SPILO_IMAGE ?= $(SPILO_REPO):latest
SPILO_UPGRADE_FROM_IMAGE ?= $(SPILO_REPO):$$(cat HYDRA_PROD_VER)

# Runs the spilo acceptance tests
.PHONY: spilo_acceptance_test
spilo_acceptance_test: $(TEST_ARTIFACT_DIR)
	export ARTIFACT_DIR=$(TEST_ARTIFACT_DIR) && \
		export SPILO_IMAGE=$(SPILO_IMAGE) && \
		export SPILO_UPGRADE_FROM_IMAGE=$(SPILO_UPGRADE_FROM_IMAGE) && \
		cd acceptance && \
		go test ./spilo/... $(GO_TEST_FLAGS) -count=1 -v

.PHONY: spilo_pull_upgrade_image
spilo_pull_upgrade_image: ecr_login
	docker pull $(SPILO_UPGRADE_FROM_IMAGE)

.PHONY: spilo_acceptance_build_test
# Builds the spilo image then runs acceptance tests
spilo_acceptance_build_test: docker_build_local_spilo spilo_pull_upgrade_image spilo_acceptance_test

.PHONY: lint_acceptance
# Runs the go linter
lint_acceptance:
	docker run --rm -v $(CURDIR)/acceptance:/app -w /app golangci/golangci-lint:$(GOLANGCI_LINT_VERSION) \
		golangci-lint run --timeout 5m --out-format colored-line-number

.PHONY: lint_fix_acceptance
# Runs the go linter with the auto-fixer
lint_fix_acceptance:
	docker run --rm -v $(CURDIR)/acceptance:/app -w /app golangci/golangci-lint:$(GOLANGCI_LINT_VERSION) \
		golangci-lint run --fix

BENCH_DSN ?= postgresql://postgres:postgres@127.0.0.1:5432/postgres
BENCH_ARGS ?=
CLICKHOUSE_IMAGE ?= clickhouse/clickhouse-server:25.3
CLICKHOUSE_CONTAINER ?= clickhouse_bench
CLICKHOUSE_USER ?= default
CLICKHOUSE_PASSWORD ?= clickhouse
CLICKHOUSE_HTTP_PORT ?= 8123
CLICKHOUSE_TCP_PORT ?= 9000
CLICKHOUSE_DSN ?= http://$(CLICKHOUSE_USER):$(CLICKHOUSE_PASSWORD)@127.0.0.1:$(CLICKHOUSE_HTTP_PORT)/default
ALLOYDB_DSN ?= postgresql://postgres:notofox@127.0.0.1:5434/postgres
ALLOYDB_OMNI_IMAGE ?= google/alloydbomni
ALLOYDB_COLUMNAR_CONTAINER ?= alloydb-omni-columnar
ALLOYDB_COLUMNAR_PASSWORD ?= notofox
ALLOYDB_COLUMNAR_PORT ?= 5444
ALLOYDB_COLUMNAR_MEMORY_MB ?= 2048
ALLOYDB_COLUMNAR_SHM_SIZE ?= 2g
ALLOYDB_COLUMNAR_DSN ?= postgresql://postgres:$(ALLOYDB_COLUMNAR_PASSWORD)@127.0.0.1:$(ALLOYDB_COLUMNAR_PORT)/postgres

.PHONY: bench_storage
bench_storage:
	python3 bench/local_storage_benchmark.py --dsn "$(BENCH_DSN)" $(BENCH_ARGS)

.PHONY: bench_storage_smoke
bench_storage_smoke:
	python3 bench/local_storage_benchmark.py --dsn "$(BENCH_DSN)" --rows 50000 --query-runs 1 --append-batches 3 --append-rows 1000 $(BENCH_ARGS)

.PHONY: clickhouse_bench_start
clickhouse_bench_start:
	-docker rm -f $(CLICKHOUSE_CONTAINER) >/dev/null 2>&1
	docker run -d --name $(CLICKHOUSE_CONTAINER) \
		--ulimit nofile=262144:262144 \
		-e CLICKHOUSE_USER=$(CLICKHOUSE_USER) \
		-e CLICKHOUSE_PASSWORD=$(CLICKHOUSE_PASSWORD) \
		-e CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT=1 \
		-p $(CLICKHOUSE_HTTP_PORT):8123 \
		-p $(CLICKHOUSE_TCP_PORT):9000 \
		$(CLICKHOUSE_IMAGE)

.PHONY: clickhouse_bench_stop
clickhouse_bench_stop:
	docker rm -f $(CLICKHOUSE_CONTAINER)

.PHONY: bench_storage_clickhouse_smoke
bench_storage_clickhouse_smoke:
	python3 bench/local_storage_benchmark.py --dsn "$(BENCH_DSN)" \
		--rows 50000 --query-runs 1 --append-batches 1 --append-rows 1000 \
		--compare-clickhouse clickhouse=$(CLICKHOUSE_DSN) \
		$(BENCH_ARGS)

.PHONY: bench_storage_clickhouse_25m
bench_storage_clickhouse_25m:
	python3 bench/local_storage_benchmark.py --dsn "$(BENCH_DSN)" \
		--layouts columnar --rows 25000000 --query-runs 3 \
		--append-batches 5 --append-rows 10000 \
		--compare-clickhouse clickhouse=$(CLICKHOUSE_DSN) \
		--output tmp/clickhouse-25m.json \
		$(BENCH_ARGS)

.PHONY: alloydb_columnar_bench_start
alloydb_columnar_bench_start:
	-docker rm -f $(ALLOYDB_COLUMNAR_CONTAINER) >/dev/null 2>&1
	docker run -d --name $(ALLOYDB_COLUMNAR_CONTAINER) \
		--shm-size=$(ALLOYDB_COLUMNAR_SHM_SIZE) \
		-e POSTGRES_PASSWORD=$(ALLOYDB_COLUMNAR_PASSWORD) \
		-p $(ALLOYDB_COLUMNAR_PORT):5432 \
		$(ALLOYDB_OMNI_IMAGE) \
		postgres -c google_columnar_engine.enabled=on \
			-c google_columnar_engine.memory_size_in_mb=$(ALLOYDB_COLUMNAR_MEMORY_MB)

.PHONY: alloydb_columnar_bench_stop
alloydb_columnar_bench_stop:
	docker rm -f $(ALLOYDB_COLUMNAR_CONTAINER)

.PHONY: bench_storage_alloydb_columnar_smoke
bench_storage_alloydb_columnar_smoke:
	python3 bench/local_storage_benchmark.py --dsn "$(BENCH_DSN)" \
		--layouts columnar --rows 50000 --query-runs 1 \
		--append-batches 1 --append-rows 1000 \
		--compare alloydb=$(ALLOYDB_DSN) \
		--compare-alloydb-columnar alloydb_columnar=$(ALLOYDB_COLUMNAR_DSN) \
		--output tmp/alloydb-columnar-smoke.json \
		$(BENCH_ARGS)

.PHONY: bench_storage_alloydb_columnar_25m
bench_storage_alloydb_columnar_25m:
	python3 bench/local_storage_benchmark.py --dsn "$(BENCH_DSN)" \
		--layouts columnar --rows 25000000 --query-runs 3 \
		--append-batches 5 --append-rows 10000 \
		--compare alloydb=$(ALLOYDB_DSN) \
		--compare-alloydb-columnar alloydb_columnar=$(ALLOYDB_COLUMNAR_DSN) \
		--output tmp/alloydb-columnar-25m.json \
		$(BENCH_ARGS)

VECTOR_BENCH_ARGS ?=
PG_MAJOR ?= 18
PGVECTOR_REF ?= v0.8.2
PGVECTORSCALE_REPO ?= https://github.com/ryrobes/pgvectorscale.git
PGVECTORSCALE_REF ?= main

.PHONY: bench_vector
bench_vector:
	python3 bench/vector_benchmark.py $(VECTOR_BENCH_ARGS)

.PHONY: image_pgvector_baseline
image_pgvector_baseline:
	docker build -f bench/Dockerfile.pgvector \
		--build-arg PG_MAJOR=$(PG_MAJOR) \
		--build-arg PGVECTOR_REF=$(PGVECTOR_REF) \
		-t postgres$(PG_MAJOR)-pgvector:$(PGVECTOR_REF) .

.PHONY: image_pgvectorscale_baseline
image_pgvectorscale_baseline:
	docker build -f bench/Dockerfile.pgvectorscale \
		--build-arg PG_MAJOR=$(PG_MAJOR) \
		--build-arg PGVECTOR_REF=$(PGVECTOR_REF) \
		--build-arg PGVECTORSCALE_REPO=$(PGVECTORSCALE_REPO) \
		--build-arg PGVECTORSCALE_REF=$(PGVECTORSCALE_REF) \
		-t postgres$(PG_MAJOR)-pgvectorscale:$(PGVECTORSCALE_REF) .

##
## Distribution image — a self-contained PG18 + columnar + pgvector image
## that can be pushed to a registry and run without this repo.
##

# Override these if you want to push to your own registry
IMAGE_REPO ?= ryrobes/hydra-columnar-pg18
IMAGE_TAG  ?= latest
IMAGE      := $(IMAGE_REPO):$(IMAGE_TAG)

.PHONY: image_build
# Build the standalone distribution image.
image_build:
	docker build -f Dockerfile.pg18 -t $(IMAGE) -t $(IMAGE_REPO):pg18 .

.PHONY: image_build_multiarch
# Build for linux/amd64 and linux/arm64 via buildx (requires `docker buildx create --use` once).
image_build_multiarch:
	docker buildx build -f Dockerfile.pg18 \
		--platform linux/amd64,linux/arm64 \
		-t $(IMAGE) -t $(IMAGE_REPO):pg18 \
		--push .

.PHONY: image_push
# Push the locally-built image to the configured registry.
image_push:
	docker push $(IMAGE)
	docker push $(IMAGE_REPO):pg18

.PHONY: image_run
# Run the image locally (no repo needed once the image is built/pulled).
image_run:
	docker run -d --name columnar-pg18 \
		-p 5432:5432 \
		-e POSTGRES_USER=postgres \
		-e POSTGRES_PASSWORD=postgres \
		-e POSTGRES_DB=postgres \
		-e COLUMNAR_DEFAULT_TABLE_ACCESS_METHOD=columnar \
		-v columnar_pg18_data:/var/lib/postgresql \
		$(IMAGE)

.PHONY: image_stop
image_stop:
	-docker stop columnar-pg18
	-docker rm columnar-pg18
