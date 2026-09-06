.PHONY: build test codegen run clean test-compat docker-build docker-run changelog stats docs-serve docs-build

build:
	go build -o dist/devcloud ./cmd/devcloud
	go build -o dist/codegen ./cmd/codegen

test:
	CGO_ENABLED=0 go test ./... -v

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

# Serves docs/ as the site published to skyoo2003.github.io/devcloud.
# Requires the extended edition (the theme compiles SCSS) and the theme
# submodule: git submodule update --init
docs-serve:
	hugo server --buildDrafts

# What .github/workflows/pages.yml runs. The link check is not optional: the
# render hook resolves unknown links to GitHub URLs instead of failing, so a
# renamed page breaks quietly without it.
docs-build:
	hugo --gc --minify
	./scripts/check-site-links.py

clean:
	rm -rf dist/ data/ public/ resources/

changelog:
	@if [ -z "$(VERSION)" ]; then \
	  echo "VERSION is required. Usage: make changelog VERSION=v0.2.0"; \
	  exit 1; \
	fi
	@changie batch $(VERSION) && changie merge

stats:
	@svcs=$$(grep -rho 'DefaultRegistry.Register(' internal/services | wc -l | tr -d ' '); \
	ops=$$(grep -r 'case "' internal/services/*/provider.go 2>/dev/null | wc -l | tr -d ' '); \
	echo "Services: $$svcs"; \
	echo "Operations: $$ops"
