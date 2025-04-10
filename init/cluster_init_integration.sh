#!/bin/bash

# Don't install the integration on executors

if [ "$DB_IS_DRIVER" != "TRUE" ]; then
  exit
fi

# Define the version, download dir and target dir
NEW_RELIC_DATABRICKS_TMP_DIR="/tmp/newrelic-databricks-integration"
NEW_RELIC_DATABRICKS_TARGET_DIR="/databricks/driver/newrelic"
NEW_RELIC_DATABRICKS_RELEASE_ARCHIVE="newrelic-databricks-integration_Linux_x86_64.tar.gz"

# Download the newrelic databricks integration release archive, unpack it, and
# move the binary into place
mkdir -p $NEW_RELIC_DATABRICKS_TMP_DIR
curl -o $NEW_RELIC_DATABRICKS_TMP_DIR/$NEW_RELIC_DATABRICKS_RELEASE_ARCHIVE -L https://github.com/newrelic/newrelic-databricks-integration/releases/latest/download/$NEW_RELIC_DATABRICKS_RELEASE_ARCHIVE
cd $NEW_RELIC_DATABRICKS_TMP_DIR && tar zvxf $NEW_RELIC_DATABRICKS_RELEASE_ARCHIVE
mkdir -p $NEW_RELIC_DATABRICKS_TARGET_DIR
cp $NEW_RELIC_DATABRICKS_TMP_DIR/newrelic-databricks-integration $NEW_RELIC_DATABRICKS_TARGET_DIR
cd $NEW_RELIC_DATABRICKS_TARGET_DIR && mkdir -p configs

# Create the integration configuration file
sudo cat <<EOF > $NEW_RELIC_DATABRICKS_TARGET_DIR/configs/config.yml
apiKey: $NEW_RELIC_API_KEY
licenseKey: $NEW_RELIC_LICENSE_KEY
accountId: $NEW_RELIC_ACCOUNT_ID
region: $NEW_RELIC_REGION
interval: 30
runAsService: true
databricks:
  workspaceHost: $NEW_RELIC_DATABRICKS_WORKSPACE_HOST
  accessToken: "$NEW_RELIC_DATABRICKS_ACCESS_TOKEN"
  oauthClientId: "$NEW_RELIC_DATABRICKS_OAUTH_CLIENT_ID"
  oauthClientSecret: "$NEW_RELIC_DATABRICKS_OAUTH_CLIENT_SECRET"
  usage:
    enabled: $NEW_RELIC_DATABRICKS_USAGE_ENABLED
    warehouseId: "$NEW_RELIC_DATABRICKS_SQL_WAREHOUSE"
    includeIdentityMetadata: false
    runTime: 02:00:00
  jobs:
    runs:
      enabled: $NEW_RELIC_DATABRICKS_JOB_RUNS_ENABLED
      metricPrefix: databricks.
      includeIdentityMetadata: false
      includeRunId: false
      startOffset: 86400
  pipelines:
    metrics:
      enabled: $NEW_RELIC_DATABRICKS_PIPELINE_METRICS_ENABLED
      metricPrefix: databricks.
      includeUpdateId: false
      startOffset: 86400
      intervalOffset: 5
    logs:
      enabled: $NEW_RELIC_DATABRICKS_PIPELINE_EVENT_LOGS_ENABLED
spark:
  webUiUrl: http://{UI_HOST}:{UI_PORT}
  metricPrefix: spark.
tags:
  databricksClusterId: $DB_CLUSTER_ID
  databricksClusterName: $DB_CLUSTER_NAME
EOF

chmod 600 $NEW_RELIC_DATABRICKS_TARGET_DIR/configs/config.yml

# Create integration startup file
sudo cat <<EOM > $NEW_RELIC_DATABRICKS_TARGET_DIR/start-integration.sh
#!/bin/bash

TRIES=0

while [ \$TRIES -lt 5 ]; do
  if [ ! -e /tmp/driver-env.sh ]; then
    sleep 5
  fi

  TRIES=\$((TRIES + 1))
done

if [ ! -e /tmp/driver-env.sh ]; then
  echo Integration failed to start - missing /tmp/driver-env.sh
  exit 1
fi

source /tmp/driver-env.sh

sed -i "s/{UI_HOST}/\$CONF_PUBLIC_DNS/;s/{UI_PORT}/\$CONF_UI_PORT/" \
  $NEW_RELIC_DATABRICKS_TARGET_DIR/configs/config.yml

$NEW_RELIC_DATABRICKS_TARGET_DIR/newrelic-databricks-integration
EOM

chmod 500 $NEW_RELIC_DATABRICKS_TARGET_DIR/start-integration.sh

# Create systemd service file
sudo cat <<EOM > /etc/systemd/system/newrelic-databricks-integration.service
[Unit]
Description=New Relic Databricks Integration
After=network.target

[Service]
RuntimeDirectory=newrelic-databricks-integration
WorkingDirectory=$NEW_RELIC_DATABRICKS_TARGET_DIR
Type=exec
ExecStart=$NEW_RELIC_DATABRICKS_TARGET_DIR/start-integration.sh
MemoryLimit=1G
# MemoryMax is only supported in systemd > 230 and replaces MemoryLimit. Some cloud dists do not have that version
# MemoryMax=1G
Restart=always
RestartSec=30
StartLimitInterval=200
StartLimitBurst=5
PIDFile=/run/newrelic-databricks-integration/newrelic-databricks-integration.pid

[Install]
WantedBy=multi-user.target
EOM

# Enable and run the service
sudo systemctl daemon-reload
sudo systemctl enable newrelic-databricks-integration.service
sudo systemctl start newrelic-databricks-integration
