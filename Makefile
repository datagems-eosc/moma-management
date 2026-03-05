
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