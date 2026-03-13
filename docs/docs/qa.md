# Quality Assurance

Key aspects of the Quality Assurance checklist that DataGEMS services must pass have been defined in the processes and documents governing the platform development and quality assurance. This section describes the QA practices applied to the MoMa Management API.

## Testing

The service uses [pytest](https://docs.pytest.org/) for automated testing. Tests use [testcontainers](https://testcontainers-python.readthedocs.io/) to spin up a real Neo4j instance automatically — no external services need to be pre-configured.

```bash
# Install all dependency groups
uv sync --all-groups

# Run the full test suite (4 parallel workers)
uv run pytest
```

Test files are in the `tests/` folder:

| File | What it tests |
|---|---|
| `test_croissant_to_moma_engine.py` | Croissant → PG-JSON mapping engine |
| `test_dataset_service.py` | Dataset service business logic |
| `test_dataset_storage.py` | Neo4j dataset repository (integration) |
| `test_node_storage.py` | Neo4j node repository (integration) |
| `test_api.py` | End-to-end API tests |

Tests are also run automatically on every push and pull request via the [CI workflow](automations.md).

## Static code analysis

Static code analysis helps identify potential errors, vulnerabilities, and maintainability issues without executing the code. The repository is configured to enable GitHub CodeQL scanning for security and quality issues.

## Vulnerability checks

Docker image vulnerability scanning can be performed using [Trivy](https://trivy.dev/) or equivalent tooling against the versioned images published to the GitHub Container Registry.

## Postman collection

A Postman collection for manual and exploratory API testing is available in the repository at `tests/api/moma_api.postman_collection.json`. To use it, create a Postman environment with the following variables:

- `baseUrl`: the API endpoint (e.g. `http://localhost:5000`)
- `userAccessToken`: a valid Bearer token obtained from the DataGEMS AAI service

## Code Metrics

Code metrics are quantitative measurements used to assess various aspects of source code quality and complexity. They help developers understand how maintainable, efficient, and error-prone a codebase might be. Common code metrics include lines of code (LOC), cyclomatic complexity, maintenability index, and coupling levels. By analyzing these metrics, teams can identify potential issues, enforce coding standards, and improve overall software quality throughout the development lifecycle.

The service has configured an automated GitHub Actions workflow, as described in the relevant [Automations](automations.md) section to calculate such metrics.

## Vulnerability checks

Vulnerability checks are processes used to identify known security weaknesses in software, libraries, or dependencies. These checks typically scan the codebase, configuration files, or external packages against databases of publicly disclosed vulnerabilities. By detecting issues such as outdated libraries, insecure functions, or misconfigurations, vulnerability checks help developers address security risks early and maintain a secure software environment.

The service has configured an automated GitHub Actions workflow, as described in the relevant [Automations](automations.md) section to perform such checks on the versioned artefacts.

## Testing

As the Gateway API mainly provides proxying, aggregation and transformation logic over the underpinning DataGEMS services, an API testing approach has been choosen to perform smoke testing on existing installations of the API, be it in local development environemnt or deployed instances. The testing approach allows the usage of the same testing scenarios while in development as well as in deployed installations. It reuses the internal integration mechanisms provided to facilitate development and communication by reusing Postman request collections.

The testing collections can be found in the service [code repository](https://github.com/datagems-eosc/dg-app-api) in the [tests folder](https://github.com/datagems-eosc/dg-app-api/tree/main/tests).

The test collections can be executed locally using the [Postman](https://www.postman.com/) application, or by command line using the [Newman CLI](https://github.com/postmanlabs/newman). To run the tests locally, one can use the following command using docker with a terminal inside the tests folder. It can also be run without docker after installing the newman library locally.

```console
docker run --rm -v "${PWD}:/etc/newman" postman/newman:6-ubuntu run DataGEMS-GatewayApi-Tests.postman_collection.json --env-var "baseUrl=<...>" --env-var "..." --reporters cli
```

A test user must be used in order to login and permorm the authenticated operations.

The testing collection can also be executed through the CI pipelines as described in the relevant [Automations](automations.md) section.

The output of the test run presents a summary of the requests performed and any errors that may have been observed.

```
┌───────────────────────┬────────────────┬───────────────┐
│                       │       executed │        failed │
├───────────────────────┼────────────────┼───────────────┤
│            iterations │              1 │             0 │
├───────────────────────┼────────────────┼───────────────┤
│              requests │             16 │             0 │
├───────────────────────┼────────────────┼───────────────┤
│          test-scripts │             16 │             0 │
├───────────────────────┼────────────────┼───────────────┤
│    prerequest-scripts │              6 │             0 │
├───────────────────────┼────────────────┼───────────────┤
│            assertions │             16 │             0 │
├───────────────────────┴────────────────┴───────────────┤
│ total run duration: 3.3s                               │
├────────────────────────────────────────────────────────┤
│ total data received: 29.96kB (approx)                  │
├────────────────────────────────────────────────────────┤
│ avg resp time:193ms [min:103ms, max:358ms, s.d.:100ms] │
└────────────────────────────────────────────────────────┘
```
