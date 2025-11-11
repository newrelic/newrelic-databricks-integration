# Additional Information

## Example: Creating and Using a Secret for your New Relic License Key

Sensitive data like credentials and API keys should never be specified directly
in custom [environment variables](https://docs.databricks.com/aws/en/compute/configure#environment-variables).
Instead, it is recommended to create a [secret](https://docs.databricks.com/en/security/secrets/secrets)
and [reference the secret in the environment variable](https://docs.databricks.com/aws/en/security/secrets/secrets-spark-conf-env-var#reference-a-secret-in-an-environment-variable).

For example, the [init script](../init/cluster_init_integration.sh) used to
install the Databricks Integration when [deploying the integration to a Databricks cluster](./installation.md#deploy-the-integration-to-a-databricks-cluster)
requires the New Relic license key to be specified in the environment variable
named `NEW_RELIC_LICENSE_KEY`. The steps below show an example of using the [Databricks CLI](https://docs.databricks.com/aws/en/dev-tools/cli/)
to create a [secret](https://docs.databricks.com/en/security/secrets/secrets.html)
for the New Relic license key and [reference it in a custom environment variable](https://docs.databricks.com/aws/en/security/secrets/secrets-spark-conf-env-var#reference-a-secret-in-an-environment-variable).
These steps can be repeated for each custom [environment variable](https://docs.databricks.com/aws/en/compute/configure#environment-variables)
that contains sensitive data.

1. Follow the steps to [install or update the Databricks CLI](https://docs.databricks.com/en/dev-tools/cli/install).
1. Use the [Databricks CLI](https://docs.databricks.com/aws/en/dev-tools/cli/)
   to create a [Databricks-backed secret scope](https://docs.databricks.com/aws/en/security/secrets#manage-secret-scopes)
   with the name `newrelic`. For example,

   ```bash
   databricks secrets create-scope newrelic
   ```

   **NOTE:** This step can also be done using the [Databricks workspace UI](https://docs.databricks.com/aws/en/security/secrets/?language=Databricks%C2%A0workspace%C2%A0UI#create-a-secret-scope).
1. Use the [Databricks CLI](https://docs.databricks.com/aws/en/dev-tools/cli/)
   to [create a secret](https://docs.databricks.com/aws/en/security/secrets#create-a-secret)
   for the license key in the new scope with the name `licenseKey`. For example,

   ```bash
   databricks secrets put-secret --json '{
      "scope": "newrelic",
      "key": "licenseKey",
      "string_value": "[YOUR_LICENSE_KEY]"
   }'
   ```
1. Set the custom environment variable named `NEW_RELIC_LICENSE_KEY` and
   reference the value from the secret by following the steps to
   [configure custom environment variables](https://docs.databricks.com/en/compute/configure#environment-variables),
   adding the following line after the last entry in the `Environment variables`
   field.

   `NEW_RELIC_LICENSE_KEY={{secrets/newrelic/licenseKey}}`

## Notes

* All references within the [Databricks Integration documentation](https://github.com/newrelic/newrelic-databricks-integration?tab=readme-ov-file#databricks-integration)
  to Databricks documentation reference the [Databricks on AWS documentation](https://docs.databricks.com/aws/en).
  Use the cloud switcher menu located in the upper right hand corner of the
  Databricks documentation to select the corresponding documentation for a
  different cloud provider.
