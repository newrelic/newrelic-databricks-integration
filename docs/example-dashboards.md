# Example Dashboards

The following example dashboard JSON files are included in the [`./examples`](./examples/)
directory. These can be imported to create dashboards that showcase each of the
main features of the Databricks Integration.

-   [Apache Spark](./examples/spark-daskboard.json)
-   [Databricks Job Runs](./examples/job-runs-dashboard.json)
-   [Databricks Pipeline Updates](./examples/pipeline-updates-dashboard.json)
-   [Databricks Queries](./examples/query-metrics-dashboard.json)
-   [Databricks Cluster Health](./examples/cluster-health-dashboard.json)
-   [Databricks Consumption & Cost](./examples/consumption-cost-dashboard.json)

**NOTE:** Collection of data for some dashboards requires additional
configuration. See the appropriate feature documentation for more details.

To import these dashboards, perform the following steps within New Relic for
each dashboard.

1. Open the JSON file for the dashboard to import.
2. Locate each instance of the following text and change the account ID `0` to
   the account ID of the account where the dashboard will be installed.

    ```json
    "accountIds": [
      0
    ],
    ```

    For example, to install the dashboard to account 12345, all instances of
    the above text would be changed to the following:

    ```json
    "accountIds": [
      12345
    ],
    ```

3. Click on "Dashboards" from the left navigation in New Relic.
4. Click on the button labeled "Import dashboard" in the upper-right hand corner
   and copy and paste the contents of the modified dashboard JSON file into the
   text field labeled "Dashboard JSON".
5. Choose the permission setting and the destination account for the dashboard.
6. Click on the button labeled "Import dashboard".
7. In the "Dashboard created" notification that appears, click on the button
   labeled "See it" to view the dashboard, or refresh the screen and click on
   the name of the imported dashboard in the table.
