
gen:
	rm -rf moma_management/domain/generated && mkdir -p moma_management/domain/generated
	uv run datamodel-codegen \
	  --input moma_management/domain/schema \
	  --input-file-type jsonschema \
	  --output moma_management/domain/generated \
	  --reuse-model \
	  --collapse-root-models \
	  --use-title-as-name \
	  --output-model-type pydantic_v2.BaseModel \
	  --use-standard-collections \
	  --use-field-description \
	  --snake-case-field

# Hand the trained adapter to the publish-analytical-pattern-embedder workflow, which merges
# it and publishes the model on Hugging Face:
#     make release-embedder VERSION=1 NOTES="Why it was retrained."
EMBEDDER := packages/analytical-pattern-embedder
ADAPTER := $(EMBEDDER)/models/analytical_pattern_embedder
release-embedder:
	@test -n "$(VERSION)" || { echo 'Usage: make release-embedder VERSION=1 NOTES="Why it was retrained."'; exit 1; }
	@grep -q '"revision": "' $(ADAPTER)/adapter_config.json || { echo "No adapter trained on a pinned base model in $(ADAPTER): run train.py."; exit 1; }
	@ls $(EMBEDDER)/.benchmarks/*.md > /dev/null || { echo "No benchmark report: run benchmark.py."; exit 1; }
	@# The workflow runs the code of the released commit, so GitHub must have it.
	@git cat-file -e HEAD:.github/workflows/publish-analytical-pattern-embedder.yml 2> /dev/null || { echo "Commit the publish workflow first: HEAD does not have it."; exit 1; }
	@git branch -r --contains HEAD | grep -q . || { echo "Push your commits first: the release is made from HEAD."; exit 1; }
	tar -czf /tmp/analytical_pattern_embedder.tar.gz --exclude=trainer -C $(ADAPTER) .
	gh release create analytical-pattern-embedder-v$(VERSION) \
	  /tmp/analytical_pattern_embedder.tar.gz "$$(ls -t $(EMBEDDER)/.benchmarks/*.md | head -1)" \
	  --target "$$(git rev-parse HEAD)" --latest=false \
	  --title "Analytical pattern embedder v$(VERSION)" --notes "$(NOTES)"
