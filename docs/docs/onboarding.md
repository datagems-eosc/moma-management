# Onboarding Material

This section contains references and material to assist users and integrators with their onboarding process.

## References

The definitive guide for integrators is the service [OpenAPI Reference](openapi.md).

The codebase is available under the [MIT License](license.md) in the [GitHub code repository](https://github.com/datagems-eosc/moma-management).

To get a better understanding of the API structure and conventions, you can read the [API Overview](api-overview.md).

For information on HTTP response codes, see the [Status & Error Codes](error-codes.md) page.

For security topics (authentication and authorization), see the [Security](security.md) section.

For a high-level view of how the service is structured, see the [Architecture](architecture.md) page.

For any questions, consult the [FAQ](faq.md).

If you encounter a problem, check if there is a relevant [issue](https://github.com/datagems-eosc/moma-management/issues) in the repository.

You can always contact us through the [DataGEMS Help Desk](https://datagems.eu/contact-us/).

## Postman collection

A Postman collection with example API calls is available in the repository under `tests/api/moma_api.postman_collection.json`.

To use it, create a Postman environment and define:

- `baseUrl`: the API base URL (e.g. `http://localhost:5000`)
- `userAccessToken`: a valid Bearer token from the DataGEMS AAI service

## Keep in touch

Follow the DataGEMS channels for the latest news and updates:

- [GitHub](https://github.com/datagems-eosc/)
- [Instagram](https://www.instagram.com/datagems_eosc)
- [X](https://x.com/datagems_eosc)
- [YouTube](https://www.youtube.com/@DataGEMS-65n)
- [LinkedIn](https://www.linkedin.com/company/eosc-datagems)
- [Facebook](https://www.facebook.com/datagems.eosc/)

To get a better understanding on features and approaches utilized throughout the HTTP Api interface, you can read about some [Api Constructs](api-overview.md).

To know more about Api response codes and how to interpret them, you can take a look at the [Status & Error Codes](error-codes.md).

On aspect pertaining to security, you can read more at the [Secruity](security.md) section.

For an overall view of the service, you can go through the [Architecture](architecture.md).

For any questions, you can consult the [FAQ](faq.md).

If you are facing a problem, check if there is a relevant [issue](https://github.com/datagems-eosc/dg-app-api/issues) answering your question.

You can always contact us through the [DataGEMS Help Desk](https://datagems.eu/contact-us/).

## Examples

You can find here a postman collection that describes some example calls of the Api. You will need to create an environment and define the following variables:

* userAccessToken: set here the access token retrieved from the DataGEMS AAI service
* baseUrl: set this to the api endpoint you want to use

Depending on the access level of the user that was authenticated and whose access token you have placed in the *userAccessToken* variable, you will be able to utilize the respective endpoints.

The postman collection can be found here: [DataGEMS.dg-app-api.postman-collection.json](content/DataGEMS.dg-app-api.postman-collection.json).


## Tutorials

You can find useful material and onboarding guidelines in our social channels bellow, as well as our [platform documentation](https://datagems-eosc.github.io/).

## Keep in touch

Make sure to follow the DataGEMS channels to get the latest news and references:

* [GitHub](https://github.com/datagems-eosc/)
* [Instagram](https://www.instagram.com/datagems_eosc)
* [X](https://x.com/datagems_eosc)
* [YoutTube](https://www.youtube.com/@DataGEMS-65n)
* [LinkedIn](https://www.linkedin.com/company/eosc-datagems)
* [Facebook](https://www.facebook.com/datagems.eosc/)
