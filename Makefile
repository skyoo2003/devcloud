.PHONY: build test codegen run clean test-compat build-web build-all docker-build docker-run changelog

build:
	go build -o dist/devcloud ./cmd/devcloud
	go build -o dist/codegen ./cmd/codegen

build-web:
	cd web && npm run build

build-all: build-web build

test:
	CGO_ENABLED=1 go test ./... -v

test-compat:
	cd tests/compatibility && pip install -q -r requirements.txt && pytest -v

codegen:
	go run ./cmd/codegen -models ./smithy-models -output ./internal/generated -templates ./internal/codegen/templates

codegen-s3:
	go run ./cmd/codegen -models ./smithy-models -output ./internal/generated -services s3 -templates ./internal/codegen/templates

run:
	go run ./cmd/devcloud

docker-build:
	docker build -f docker/Dockerfile -t devcloud/devcloud .

docker-run:
	docker run -p 4747:4747 -v $(PWD)/data:/app/data devcloud/devcloud

clean:
	rm -rf dist/ data/

changelog:
	@if [ -z "$(VERSION)" ]; then \
	  echo "VERSION is required. Usage: make changelog VERSION=v0.2.0"; \
	  exit 1; \
	fi
	@changie batch $(VERSION) && changie merge
