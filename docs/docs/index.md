# DataGEMS MoMa Management API

This is the documentation site for the **MoMa Management API**, a service that is part of the wider [DataGEMS](https://datagems.eu/) platform. See also the overall [platform documentation](https://datagems-eosc.github.io/).

The MoMa Management API is responsible for ingesting, storing, and exposing **Metadata Object Model (MoMa)** property graphs in [Neo4j](https://neo4j.com/). It accepts dataset profiles in [Croissant](https://docs.mlcommons.org/croissant/docs/) format, converts them to PG-JSON according to the MoMa schema, and persists the result. It also exposes endpoints to retrieve and update individual graph nodes.

You can use the menu options on the left to navigate through the available documentation. You may be interested to jump directly to an [Architecture Overview](architecture.md), see the available [Onboarding Material](onboarding.md), read about the [API Constructs](api-overview.md), investigate how to interpret [Status & Error Codes](error-codes.md) or jump straight in to the [OpenAPI Reference](openapi.md). The service [code repository](https://github.com/datagems-eosc/moma-management) is publicly available on GitHub.

For any questions, you can consult the [FAQ](faq.md), check if there is a relevant [issue](https://github.com/datagems-eosc/moma-management/issues) answering your question or contact the [DataGEMS Help Desk](https://datagems.eu/contact-us/).
