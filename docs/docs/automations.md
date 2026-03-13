# Automations

A number of GitHub Actions workflows automate development, quality assurance, and deployment for the service. All workflows are defined in the [`.github/workflows`](https://github.com/datagems-eosc/moma-management/tree/main/.github/workflows) directory of the repository.

## Test workflow

Workflow file: [`tests.yml`](https://github.com/datagems-eosc/moma-management/blob/main/.github/workflows/tests.yml)

Triggered automatically on every push and pull request targeting the `main` or `develop` branches. The workflow:

1. Checks out the repository.
2. Installs Python 3.14 and [uv](https://docs.astral.sh/uv/).
3. Installs all dependency groups (`uv sync --all-groups`).
4. Runs the full pytest suite (`uv run pytest`).

Tests use [testcontainers](https://testcontainers-python.readthedocs.io/) to spin up a real Neo4j instance automatically — no external services need to be pre-configured.

## Docker image publishing

Workflow file: [`docker-image.yml`](https://github.com/datagems-eosc/moma-management/blob/main/.github/workflows/docker-image.yml)

Triggered when a tag matching `v*` is pushed to the repository. The workflow:

1. Builds the `test` Docker target and runs the test suite inside the container.
2. If tests pass, builds and pushes the `prod` image to the GitHub Container Registry (`ghcr.io/datagems-eosc/moma-management`) tagged with the version that triggered the run.

## Release creation

Workflow file: [`release.yml`](https://github.com/datagems-eosc/moma-management/blob/main/.github/workflows/release.yml)

Triggered alongside the Docker publish workflow when a `v*` tag is pushed. Creates a GitHub Release named after the tag and populates the release notes from `CHANGELOG.md`.

## Documentation deployment

Workflow file: [`deploy-docs-on-demand.yml`](https://github.com/datagems-eosc/moma-management/blob/main/.github/workflows/deploy-docs-on-demand.yml)

Triggered manually via `workflow_dispatch`. Accepts a documentation version string as input. The workflow:

1. Installs [mkdocs-material](https://squidfunk.github.io/mkdocs-material/), [mike](https://github.com/jimporter/mike), [neoteroi-mkdocs](https://www.neoteroi.dev/mkdocs-plugins/), and related plugins.
2. Builds the versioned documentation site.
3. Pushes the generated site to the `gh-pages` branch, making it available at [https://datagems-eosc.github.io/moma-management/](https://datagems-eosc.github.io/moma-management/).

## Docker image publishing

A GitHub Action workflow is available to [build and publish](https://github.com/datagems-eosc/dg-app-api/blob/main/.github/workflows/docker-publish.yml) the generated docker image for the service. The action is triggered when a new tag is created in the repository with a pattern of v*. This way, all the images produced are always named and can be traced back to the codebase snapshot that generated them.

The generated docker image is pushed to the GitHub organization Packages using the name of the service repo and the version tag that triggered the execution.

## Vulnerability Scanning

A GitHub Action workflow is available to [scan for vulnerabilities](https://github.com/datagems-eosc/dg-app-api/blob/main/.github/workflows/vulnerability-scan-on-demand.yml) any docker image that was created for the service. The action is triggered manually and expects as input the version tag that was used to generate the respective docker image that must be scanned.

Vulnerability scanning is performed using [Trivy](https://trivy.dev/). The results are generated in SARIF format and are made available in the GitHub Code Scanning tool that is configured for the service.

Trivy is utilized to scan both the configuration that triggers the Docker image generation (ie the Dockerfile) as well as the generated image. The scanning performed on the generated image includes both OS vulnerabilities as well as installed libraries.

## Static Code Analysis

A GitHub Action workflow is available to [analyze the code](https://github.com/datagems-eosc/dg-app-api/blob/main/.github/workflows/codeql-scan-on-demand.yml) using static code analysis offered through GitHub's CodeQL. The action is triggered manually and can be executed against the HEAD of the repository.

The scan is configured to evaluate rules on security, quality and maintenability of the codebase. The results generated are made available in the GitHub Code Scanning tool that is configured for the service.

## Code Metrics

A GitHub Action workflow is available to generate [code metrics](https://github.com/datagems-eosc/dg-app-api/blob/main/.github/workflows/code-metrics-on-demand.yml) using ASP.NET Core msbuild targets. The action is triggered manually and can be executed against the HEAD of the repository.

The generated metrics includes useful insight on metrics such as:
* Maintenability index
* Cyclomatic Complexity
* Class Coupling
* Depth of Inheritance
* Lines of code

The metrics are generated for the hierarchy of the code base, including Assembly, Namespace, Class, Method. This provides navigable insight. The results generated are in custom msbuild xml format and are available as action artefacts.

## Documentation

A GitHub Action workflow is available to [generate documentation](https://github.com/datagems-eosc/dg-app-api/blob/main/.github/workflows/deploy-docs-on-demand.yml) available in the project in the format presented here. The action is triggered manually and can be executed against the head of the repository. The documentatino generated is versioned and the documentation version is expected as input to the workflow.

The documentation is build using the [mkdocs](https://www.mkdocs.org/) library and specifically using the [Material for mkdocs](https://squidfunk.github.io/mkdocs-material/) plugin. A number of additional tools are ustilized, such as [mike](https://github.com/jimporter/mike) to support versioning, [neoteroi.mkdocsoad](https://www.neoteroi.dev/mkdocs-plugins/web/oad/) to support OpenAPI specification rendering, and others.

The documentation is generated and tagged with the provided version. It is uploaded to a dedicated documentation branch that is configured to be used as the base branch over which the repository GitHub Page presents its contents.

## Testing

A GitHub Action workflow is available to [perform API smoke testing](https://github.com/datagems-eosc/dg-app-api/blob/main/.github/workflows/test-on-demand.yml) any configured installation of the service. The action is triggered manually and expects as input the version tag that contain the version of the tests that are compatible with the version of the deployed installation under test.

Smoke testing of the APIs is perfomed using a [Postman](https://www.postman.com/) collection of requests. Configuration that needs to be passed in a protected way is maintained as GitHub Secrets. The request collection is axecuted using the [Newman CLI](https://github.com/postmanlabs/newman). The output of the test run presents a summary of the requests performed and any errors that may have been observed.
