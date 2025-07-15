#!/bin/bash

NEW_RELIC_INFRASTRUCTURE_ENABLED=${NEW_RELIC_INFRASTRUCTURE_ENABLED:-"false"}
NEW_RELIC_INFRASTRUCTURE_LOGS_ENABLED=${NEW_RELIC_INFRASTRUCTURE_LOGS_ENABLED:-"false"}

if [ "$NEW_RELIC_INFRASTRUCTURE_ENABLED" = "true" ]; then
  NEW_RELIC_INFRA_CONFIG_FILE="/etc/newrelic-infra.yml"
  NEW_RELIC_INFRA_DATABRICKS_DIR="/databricks/driver/newrelic-infra"

  mkdir -p $NEW_RELIC_INFRA_DATABRICKS_DIR

  # Add New Relic's Infrastructure Agent gpg key
  curl -fsSL https://download.newrelic.com/infrastructure_agent/gpg/newrelic-infra.gpg | sudo gpg --dearmor -o /etc/apt/trusted.gpg.d/newrelic-infra.gpg

  # Create a manifest file
  echo "deb https://download.newrelic.com/infrastructure_agent/linux/apt/ jammy main" | sudo tee -a /etc/apt/sources.list.d/newrelic-infra.list

  # Run an update
  sudo apt-get update

  # Install the New Relic Infrastructure agent
  sudo apt-get install newrelic-infra -y

  # Create the agent config file
  sudo cat <<EOM >> $NEW_RELIC_INFRA_CONFIG_FILE
license_key: $NEW_RELIC_LICENSE_KEY
log:
  level: info
  forward: true
  format: json
custom_attributes:
  databricksWorkspaceHost: $NEW_RELIC_DATABRICKS_WORKSPACE_HOST
  databricksClusterId: $DB_CLUSTER_ID
  databricksClusterName: $DB_CLUSTER_NAME
  databricksIsDriverNode: $DB_IS_DRIVER
  databricksIsJobCluster: $DB_IS_JOB_CLUSTER
EOM

  if [ "$NEW_RELIC_INFRASTRUCTURE_LOGS_ENABLED" = "true" ]; then
    DRIVER_INIT_SCRIPT_LOGS_PATH="/databricks/init_scripts"
    DRIVER_LOGS_PATH="/databricks/driver/logs"
    DRIVER_EVENT_LOGS_PATH="/databricks/driver/eventlogs/*"
    EXECUTOR_LOGS_PATH="/databricks/spark/work/app-*/*"
    WORKER_INIT_SCRIPT_LOGS_PATH="/databricks/init_scripts"

    if [ "$DB_IS_DRIVER" = "TRUE" ]; then
      sudo cat <<EOM > $NEW_RELIC_INFRA_DATABRICKS_DIR/fluentbit.conf
[INPUT]
    Name tail
    Tag stdout.data
    Path $DRIVER_LOGS_PATH/stdout
    Path_Key filePath
    Key message
    Buffer_Max_Size 128k
    Mem_Buf_Limit 16384k
    Skip_Long_Lines On

[INPUT]
    Name tail
    Tag stderr.data
    Path $DRIVER_LOGS_PATH/stderr
    Path_Key filePath
    Key message
    Buffer_Max_Size 128k
    Mem_Buf_Limit 16384k
    Skip_Long_Lines On

[INPUT]
    Name tail
    Tag log4j.data
    Path $DRIVER_LOGS_PATH/log4j-active.log
    Path_Key filePath
    Multiline On
    Parser_Firstline log4j_parser
    Buffer_Max_Size 128k
    Mem_Buf_Limit 16384k
    Skip_Long_Lines On

[INPUT]
    Name tail
    Tag init_scripts_stdout.data
    Path $DRIVER_INIT_SCRIPT_LOGS_PATH/*.stdout.log
    Path_Key filePath
    Key message
    Buffer_Max_Size 128k
    Mem_Buf_Limit 16384k
    Skip_Long_Lines On

[INPUT]
    Name tail
    Tag init_scripts_stderr.data
    Path $DRIVER_INIT_SCRIPT_LOGS_PATH/*.stderr.log
    Path_Key filePath
    Key message
    Buffer_Max_Size 128k
    Mem_Buf_Limit 16384k
    Skip_Long_Lines On

[INPUT]
    Name tail
    Tag eventlog.data
    Path $DRIVER_EVENT_LOGS_PATH/eventlog
    Path_Key filePath
    Key message
    Buffer_Max_Size 512k
    Mem_Buf_Limit 16384k
    Skip_Long_Lines On

[FILTER]
    Name record_modifier
    Match stdout.data
    Record databricksLogType driver-stdout

[FILTER]
    Name parser
    Match stdout.data
    Key_Name message
    Parser stdout_parser
    Reserve_Data On

[FILTER]
    Name record_modifier
    Match stderr.data
    Record databricksLogType driver-stderr

[FILTER]
    Name parser
    Match stderr.data
    Key_Name message
    Parser stderr_parser
    Reserve_Data On

[FILTER]
    Name record_modifier
    Match log4j.data
    Record databricksLogType driver-log4j

[FILTER]
    Name record_modifier
    Match init_scripts_stdout.data
    Record databricksLogType driver-init-script-stdout

[FILTER]
    Name record_modifier
    Match init_scripts_stderr.data
    Record databricksLogType driver-init-script-stderr

[FILTER]
    Name record_modifier
    Match eventlog.data
    Record databricksLogType spark-eventlog
EOM

      sudo cat <<EOM > $NEW_RELIC_INFRA_DATABRICKS_DIR/fluentbit-parsers.conf
[PARSER]
    Name stdout_parser
    Format regex
    Regex ^(?<time>[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{3}[+-][0-9]{4}):\s+(?<message>.*)$
    Time_Key time
    Time_Format %Y-%m-%dT%H:%M:%S.%L%z
    Time_Keep On

[PARSER]
    Name stderr_parser
    Format regex
    Regex ^(?<time>[a-zA-Z]+\s+[a-zA-Z]+\s+[0-9]+\s+[0-9]{2}:[0-9]{2}:[0-9]{2}\s+[0-9]{4})\s+(?<message>.*)$
    Time_Key time
    Time_Format %a%t%b%t%d%t%H:%M:%S%t%Y
    Time_Keep On

[PARSER]
    Name log4j_parser
    Format regex
    Regex ^(?<time>[0-9]{2}\/[0-9]{2}\/[0-9]{2}\s+[0-9]{2}:[0-9]{2}:[0-9]{2})\s+(?<level>[a-zA-Z]+)\s+(?<message>.*)$
    Time_Key time
    Time_Format %y/%m/%d%t%H:%M:%S
    Time_Keep On
EOM
    else
      sudo cat <<EOM > $NEW_RELIC_INFRA_DATABRICKS_DIR/fluentbit.conf
[INPUT]
    Name tail
    Tag stdout.data
    Path $EXECUTOR_LOGS_PATH/stdout
    Path_Key filePath
    Key message
    Buffer_Max_Size 128k
    Mem_Buf_Limit 16384k
    Skip_Long_Lines On

[INPUT]
    Name tail
    Tag stderr.data
    Path $EXECUTOR_LOGS_PATH/stderr
    Path_Key filePath
    Key message
    Buffer_Max_Size 128k
    Mem_Buf_Limit 16384k
    Skip_Long_Lines On

[INPUT]
    Name tail
    Tag init_scripts_stdout.data
    Path $WORKER_INIT_SCRIPT_LOGS_PATH/*.stdout.log
    Path_Key filePath
    Key message
    Buffer_Max_Size 128k
    Mem_Buf_Limit 16384k
    Skip_Long_Lines On

[INPUT]
    Name tail
    Tag init_scripts_stderr.data
    Path $WORKER_INIT_SCRIPT_LOGS_PATH/*.stderr.log
    Path_Key filePath
    Key message
    Buffer_Max_Size 128k
    Mem_Buf_Limit 16384k
    Skip_Long_Lines On

[FILTER]
    Name record_modifier
    Match stdout.data
    Record databricksLogType executor-stdout

[FILTER]
    Name parser
    Match stdout.data
    Key_Name message
    Parser stdout_parser
    Reserve_Data On

[FILTER]
    Name record_modifier
    Match stderr.data
    Record databricksLogType executor-stderr

[FILTER]
    Name parser
    Match stderr.data
    Key_Name message
    Parser stderr_parser
    Reserve_Data On

[FILTER]
    Name record_modifier
    Match init_scripts_stdout.data
    Record databricksLogType worker-init-script-stdout

[FILTER]
    Name record_modifier
    Match init_scripts_stderr.data
    Record databricksLogType worker-init-script-stderr
EOM

      sudo cat <<EOM > $NEW_RELIC_INFRA_DATABRICKS_DIR/fluentbit-parsers.conf
[PARSER]
    Name stdout_parser
    Format regex
    Regex ^(?<time>[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{3}[+-][0-9]{4}):\s+(?<message>.*)$
    Time_Key time
    Time_Format %Y-%m-%dT%H:%M:%S.%L%z
    Time_Keep On

[PARSER]
    Name stderr_parser
    Format regex
    Regex ^(?<time>[0-9]{2}\/[0-9]{2}\/[0-9]{2}\s+[0-9]{2}:[0-9]{2}:[0-9]{2})\s+(?<level>[a-zA-Z]+)\s+(?<message>.*)$
    Time_Key time
    Time_Format %y/%m/%d%t%H:%M:%S
    Time_Keep On
EOM
    fi

    sudo cat <<EOM > /etc/newrelic-infra/logging.d/logging.yml
logs:
- name: databricks-cluster-logs
  fluentbit:
    config_file: $NEW_RELIC_INFRA_DATABRICKS_DIR/fluentbit.conf
    parsers_file: $NEW_RELIC_INFRA_DATABRICKS_DIR/fluentbit-parsers.conf
EOM

  fi

  # Start the agent
  sudo systemctl start newrelic-infra
fi

# Don't install the integration on executors
if [ "$DB_IS_DRIVER" != "TRUE" ]; then
  exit
fi

# Set environment variables with defaults
NEW_RELIC_DATABRICKS_USAGE_ENABLED=${NEW_RELIC_DATABRICKS_USAGE_ENABLED:-"false"}
NEW_RELIC_DATABRICKS_JOB_RUNS_ENABLED=${NEW_RELIC_DATABRICKS_JOB_RUNS_ENABLED:-"true"}
NEW_RELIC_DATABRICKS_PIPELINE_METRICS_ENABLED=${NEW_RELIC_DATABRICKS_PIPELINE_METRICS_ENABLED:-"true"}
NEW_RELIC_DATABRICKS_PIPELINE_EVENT_LOGS_ENABLED=${NEW_RELIC_DATABRICKS_PIPELINE_EVENT_LOGS_ENABLED:-"true"}
NEW_RELIC_DATABRICKS_QUERY_METRICS_ENABLED=${NEW_RELIC_DATABRICKS_QUERY_METRICS_ENABLED:-"true"}

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
  queries:
    metrics:
      enabled: $NEW_RELIC_DATABRICKS_QUERY_METRICS_ENABLED
      includeIdentityMetadata: false
      startOffset: 600
      intervalOffset: 5
      maxResults: 100
spark:
  webUiUrl: http://{UI_HOST}:{UI_PORT}
  clusterManager: databricks
tags:
  databricksWorkspaceHost: $NEW_RELIC_DATABRICKS_WORKSPACE_HOST
  databricksClusterId: $DB_CLUSTER_ID
  databricksClusterName: $DB_CLUSTER_NAME
  databricksIsDriverNode: $DB_IS_DRIVER
  databricksIsJobCluster: $DB_IS_JOB_CLUSTER
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
