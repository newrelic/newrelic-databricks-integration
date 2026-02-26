#!/bin/bash

NEW_RELIC_INFRASTRUCTURE_ENABLED=${NEW_RELIC_INFRASTRUCTURE_ENABLED:-"false"}
NEW_RELIC_INFRASTRUCTURE_LOGS_ENABLED=${NEW_RELIC_INFRASTRUCTURE_LOGS_ENABLED:-"false"}

if [ "$NEW_RELIC_INFRASTRUCTURE_ENABLED" = "true" ]; then
  NEW_RELIC_INFRA_CONFIG_FILE="/etc/newrelic-infra.yml"
  NEW_RELIC_INFRA_DATABRICKS_DIR="/databricks/driver/newrelic-infra"
  NEW_RELIC_INFRASTRUCTURE_LOG_LEVEL=${NEW_RELIC_INFRASTRUCTURE_LOG_LEVEL:-"info"}

  echo "Installing New Relic Infrastructure..."

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
  echo "Creating New Relic Infrastructure agent config file..."

  sudo cat <<EOM >> $NEW_RELIC_INFRA_CONFIG_FILE
license_key: $NEW_RELIC_LICENSE_KEY
log:
  level: $NEW_RELIC_INFRASTRUCTURE_LOG_LEVEL
  forward: true
  format: json
custom_attributes:
  databricksWorkspaceHost: $NEW_RELIC_DATABRICKS_WORKSPACE_HOST
  databricksWorkspaceName: $NEW_RELIC_DATABRICKS_WORKSPACE_HOST
  databricksClusterId: $DB_CLUSTER_ID
  databricksClusterName: $DB_CLUSTER_NAME
  databricksIsDriverNode: ${DB_IS_DRIVER,,}
  databricksIsJobCluster: ${DB_IS_JOB_CLUSTER,,}
EOM

  if [ "$NEW_RELIC_INFRASTRUCTURE_LOGS_ENABLED" = "true" ]; then
    echo "Enabling New Relic Infrastructure logs..."

    DRIVER_INIT_SCRIPT_LOGS_PATH="/databricks/init_scripts"
    DRIVER_LOGS_PATH="/databricks/driver/logs"
    DRIVER_EVENT_LOGS_PATH="/databricks/driver/eventlogs/*"
    EXECUTOR_LOGS_PATH="/databricks/spark/work/app-*/*"
    WORKER_INIT_SCRIPT_LOGS_PATH="/databricks/init_scripts"

    if [ "$DB_IS_DRIVER" = "TRUE" ]; then
      echo "Creating driver Fluent Bit configuration files..."

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
    Match *
    Record databricksWorkspaceHost $NEW_RELIC_DATABRICKS_WORKSPACE_HOST
    Record databricksWorkspaceName $NEW_RELIC_DATABRICKS_WORKSPACE_HOST
    Record databricksClusterId $DB_CLUSTER_ID
    Record databricksClusterName $DB_CLUSTER_NAME
    Record databricksIsDriverNode ${DB_IS_DRIVER,,}
    Record databricksIsJobCluster ${DB_IS_JOB_CLUSTER,,}

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
      echo "Creating worker Fluent Bit configuration files..."

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
    Match *
    Record databricksWorkspaceHost $NEW_RELIC_DATABRICKS_WORKSPACE_HOST
    Record databricksWorkspaceName $NEW_RELIC_DATABRICKS_WORKSPACE_HOST
    Record databricksClusterId $DB_CLUSTER_ID
    Record databricksClusterName $DB_CLUSTER_NAME
    Record databricksIsDriverNode ${DB_IS_DRIVER,,}
    Record databricksIsJobCluster ${DB_IS_JOB_CLUSTER,,}

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
  echo "Starting New Relic Infrastructure agent..."

  sudo systemctl start newrelic-infra

  echo "New Relic Infrastructure agent installation complete."
fi

# Don't install the integration on executors
if [ "$DB_IS_DRIVER" != "TRUE" ]; then
  echo "Not installing the Databricks Integration on worker nodes"
  exit
fi

echo "Installing the Databricks Integration..."

# Set environment variables with defaults
NEW_RELIC_DATABRICKS_INTERVAL=${NEW_RELIC_DATABRICKS_INTERVAL:-30}
NEW_RELIC_REGION=${NEW_RELIC_REGION:-"US"}
NEW_RELIC_DATABRICKS_LOG_LEVEL=${NEW_RELIC_DATABRICKS_LOG_LEVEL:-"warn"}
NEW_RELIC_DATABRICKS_AZURE_MSI_ENABLED=${NEW_RELIC_DATABRICKS_AZURE_MSI_ENABLED:-"false"}
NEW_RELIC_DATABRICKS_SEND_STARTUP_LOGS_ENABLED=${NEW_RELIC_DATABRICKS_SEND_STARTUP_LOGS_ENABLED:-"false"}
NEW_RELIC_DATABRICKS_COPY_STARTUP_LOGS_ENABLED=${NEW_RELIC_DATABRICKS_COPY_STARTUP_LOGS_ENABLED:-"false"}
NEW_RELIC_DATABRICKS_USAGE_ENABLED=${NEW_RELIC_DATABRICKS_USAGE_ENABLED:-"false"}
NEW_RELIC_DATABRICKS_USAGE_INCLUDE_IDENTITY_METADATA=${NEW_RELIC_DATABRICKS_USAGE_INCLUDE_IDENTITY_METADATA:-"false"}
NEW_RELIC_DATABRICKS_USAGE_RUN_TIME=${NEW_RELIC_DATABRICKS_USAGE_RUN_TIME:-"02:00:00"}
NEW_RELIC_DATABRICKS_JOB_RUNS_ENABLED=${NEW_RELIC_DATABRICKS_JOB_RUNS_ENABLED:-"true"}
NEW_RELIC_DATABRICKS_JOB_RUNS_START_OFFSET=${NEW_RELIC_DATABRICKS_JOB_RUNS_START_OFFSET:-86400}
NEW_RELIC_DATABRICKS_PIPELINE_METRICS_ENABLED=${NEW_RELIC_DATABRICKS_PIPELINE_METRICS_ENABLED:-"true"}
NEW_RELIC_DATABRICKS_PIPELINE_METRICS_START_OFFSET=${NEW_RELIC_DATABRICKS_PIPELINE_METRICS_START_OFFSET:-86400}
NEW_RELIC_DATABRICKS_PIPELINE_EVENT_LOGS_ENABLED=${NEW_RELIC_DATABRICKS_PIPELINE_EVENT_LOGS_ENABLED:-"true"}
NEW_RELIC_DATABRICKS_QUERY_METRICS_ENABLED=${NEW_RELIC_DATABRICKS_QUERY_METRICS_ENABLED:-"true"}
NEW_RELIC_DATABRICKS_QUERY_METRICS_INCLUDE_IDENTITY_METADATA=${NEW_RELIC_DATABRICKS_QUERY_METRICS_INCLUDE_IDENTITY_METADATA:-"false"}
NEW_RELIC_DATABRICKS_QUERY_METRICS_START_OFFSET=${NEW_RELIC_DATABRICKS_QUERY_METRICS_START_OFFSET:-600}
NEW_RELIC_DATABRICKS_QUERY_METRICS_MAX_RESULTS=${NEW_RELIC_DATABRICKS_QUERY_METRICS_MAX_RESULTS:-100}
NEW_RELIC_DATABRICKS_STARTUP_RETRIES=${NEW_RELIC_DATABRICKS_STARTUP_RETRIES:-15}

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
echo "Creating the Databricks Integration config file..."

LOG_FILE_NAME_CONFIG=""
if [ "$NEW_RELIC_DATABRICKS_SEND_STARTUP_LOGS_ENABLED" = "true" ] || \
   [ "$NEW_RELIC_DATABRICKS_COPY_STARTUP_LOGS_ENABLED" = "true" ]; then
  echo "Configuring Databricks Integration to log to file /tmp/newrelic-databricks-integration.log"
  LOG_FILE_NAME_CONFIG="fileName: /tmp/newrelic-databricks-integration.log"
fi

sudo cat <<EOF > $NEW_RELIC_DATABRICKS_TARGET_DIR/configs/config.yml
apiKey: $NEW_RELIC_API_KEY
licenseKey: $NEW_RELIC_LICENSE_KEY
accountId: $NEW_RELIC_ACCOUNT_ID
region: $NEW_RELIC_REGION
interval: $NEW_RELIC_DATABRICKS_INTERVAL
runAsService: true
log:
  level: $NEW_RELIC_DATABRICKS_LOG_LEVEL
  $LOG_FILE_NAME_CONFIG
databricks:
  workspaceHost: $NEW_RELIC_DATABRICKS_WORKSPACE_HOST
  accessToken: "$NEW_RELIC_DATABRICKS_ACCESS_TOKEN"
  oauthClientId: "$NEW_RELIC_DATABRICKS_OAUTH_CLIENT_ID"
  oauthClientSecret: "$NEW_RELIC_DATABRICKS_OAUTH_CLIENT_SECRET"
  azureMsiEnabled: $NEW_RELIC_DATABRICKS_AZURE_MSI_ENABLED
  azureClientId: "$NEW_RELIC_DATABRICKS_AZURE_CLIENT_ID"
  azureClientSecret: "$NEW_RELIC_DATABRICKS_AZURE_CLIENT_SECRET"
  azureTenantId: "$NEW_RELIC_DATABRICKS_AZURE_TENANT_ID"
  azureResourceId: "$NEW_RELIC_DATABRICKS_AZURE_RESOURCE_ID"
  usage:
    enabled: $NEW_RELIC_DATABRICKS_USAGE_ENABLED
    warehouseId: "$NEW_RELIC_DATABRICKS_SQL_WAREHOUSE"
    includeIdentityMetadata: $NEW_RELIC_DATABRICKS_USAGE_INCLUDE_IDENTITY_METADATA
    runTime: $NEW_RELIC_DATABRICKS_USAGE_RUN_TIME
  jobs:
    runs:
      enabled: $NEW_RELIC_DATABRICKS_JOB_RUNS_ENABLED
      startOffset: $NEW_RELIC_DATABRICKS_JOB_RUNS_START_OFFSET
  pipelines:
    metrics:
      enabled: $NEW_RELIC_DATABRICKS_PIPELINE_METRICS_ENABLED
      startOffset: $NEW_RELIC_DATABRICKS_PIPELINE_METRICS_START_OFFSET
      intervalOffset: 5
    logs:
      enabled: $NEW_RELIC_DATABRICKS_PIPELINE_EVENT_LOGS_ENABLED
  queries:
    metrics:
      enabled: $NEW_RELIC_DATABRICKS_QUERY_METRICS_ENABLED
      includeIdentityMetadata: $NEW_RELIC_DATABRICKS_QUERY_METRICS_INCLUDE_IDENTITY_METADATA
      startOffset: $NEW_RELIC_DATABRICKS_QUERY_METRICS_START_OFFSET
      intervalOffset: 5
      maxResults: $NEW_RELIC_DATABRICKS_QUERY_METRICS_MAX_RESULTS
spark:
  webUiUrl: http://{UI_HOST}:{UI_PORT}
  clusterManager: databricks
  databricks:
    clusterId: $DB_CLUSTER_ID
EOF

chmod 400 $NEW_RELIC_DATABRICKS_TARGET_DIR/configs/config.yml

# Create the integration startup file
echo "Creating the Databricks Integration startup file..."

sudo cat <<EOM > $NEW_RELIC_DATABRICKS_TARGET_DIR/start-integration.sh
#!/bin/bash

TRIES=0

while [ \$TRIES -lt $NEW_RELIC_DATABRICKS_STARTUP_RETRIES ]; do
  echo Attempt \$((TRIES + 1)) of $NEW_RELIC_DATABRICKS_STARTUP_RETRIES to check for /tmp/driver-env.sh...
  if [ ! -e /tmp/driver-env.sh ]; then
    sleep 5
  else
    break
  fi

  TRIES=\$((TRIES + 1))
done

if [ ! -e /tmp/driver-env.sh ]; then
  echo Integration failed to locate /tmp/driver-env.sh after $NEW_RELIC_DATABRICKS_STARTUP_RETRIES attempts, exiting.
  exit 1
fi

echo Sourcing /tmp/driver-env.sh...
source /tmp/driver-env.sh

echo Updating integration config with Spark UI host and port from /tmp/driver-env.sh...
sed -i "s/{UI_HOST}/\$CONF_PUBLIC_DNS/;s/{UI_PORT}/\$CONF_UI_PORT/" \
  $NEW_RELIC_DATABRICKS_TARGET_DIR/configs/config.yml

echo Launching New Relic Databricks Integration...
$NEW_RELIC_DATABRICKS_TARGET_DIR/newrelic-databricks-integration
EOM

chmod 500 $NEW_RELIC_DATABRICKS_TARGET_DIR/start-integration.sh

if [ "$NEW_RELIC_DATABRICKS_SEND_STARTUP_LOGS_ENABLED" = "true" ] || \
   [ "$NEW_RELIC_DATABRICKS_COPY_STARTUP_LOGS_ENABLED" = "true" ]; then
  echo "Creating the Databricks Integration startup helper file..."

  sudo cat <<EOM > $NEW_RELIC_DATABRICKS_TARGET_DIR/startup-helper.sh
#!/bin/bash
MESSAGES=()
TRIES=0
RESTART_COUNTER=\$(systemctl show newrelic-databricks-integration -p NRestarts --value)

while [ \$TRIES -lt 10 ]; do
  if [ ! -e /tmp/newrelic-databricks-integration.log ]; then
    sleep 15
  else
    break
  fi

  TRIES=\$((TRIES + 1))
done

function send_messages {
  local messages=("\$@")
  local ts=\$(date +%s%3N)

  LOG_API_URL="https://log-api.newrelic.com/log/v1"
  if [ "$NEW_RELIC_REGION" = "EU" ]; then
    LOG_API_URL="https://log-api.eu.newrelic.com/log/v1"
  fi

  local payload='
[{
  "common": {
    "attributes": {
      "databricksLogType": "startup",
      "databricksStartupAttempt": "'"\$RESTART_COUNTER"'",
      "databricksWorkspaceName": "$NEW_RELIC_DATABRICKS_WORKSPACE_HOST",
      "databricksClusterId": "$DB_CLUSTER_ID",
      "databricksClusterName": "${DB_CLUSTER_NAME//\'/\'\\\'\'}",
      "databricksIsDriverNode": "${DB_IS_DRIVER,,}",
      "databricksIsJobCluster": "${DB_IS_JOB_CLUSTER,,}"
    }
  },
  "logs": [
'

  local first=true
  for message in "\${messages[@]}"; do
    if [ "\$first" = true ]; then
      first=false
    else
      payload+=','
    fi

    msg=\${message//\\\\/\\\\\\\\}
    msg=\${msg//\\"/\\\\\\"}

    payload+="
    {
      \"timestamp\": \$ts,
      \"message\": \"\$msg\"
    }"
  done

  payload+='
  ]
}]'

  curl -s -XPOST \
    -H "Api-Key: $NEW_RELIC_LICENSE_KEY" \
    -H "Content-Type: application/json" \
    -H "Accept: application/json" \
    \$LOG_API_URL \
    -d "\$payload"
}

if [ "$NEW_RELIC_DATABRICKS_COPY_STARTUP_LOGS_ENABLED" = "true" ]; then
  mkdir -p /dbfs/tmp/$DB_CLUSTER_ID
fi

if [ -f /tmp/newrelic-databricks-integration.log ]; then
  if [ "$NEW_RELIC_DATABRICKS_SEND_STARTUP_LOGS_ENABLED" = "true" ]; then
    while IFS= read -r line; do
      MESSAGES+=("\$line")
    done < <(head -n 100 /tmp/newrelic-databricks-integration.log)
  fi

  if [ "$NEW_RELIC_DATABRICKS_COPY_STARTUP_LOGS_ENABLED" = "true" ]; then
    cp /tmp/newrelic-databricks-integration.log /dbfs/tmp/$DB_CLUSTER_ID/newrelic-databricks-integration.log
  fi
elif [ "$NEW_RELIC_DATABRICKS_SEND_STARTUP_LOGS_ENABLED" = "true" ]; then
  MESSAGES+=("Integration log file not found!")
fi

if [ -f /tmp/newrelic-databricks-start-integration.sh.stdout.log ]; then
  if [ "$NEW_RELIC_DATABRICKS_SEND_STARTUP_LOGS_ENABLED" = "true" ]; then
    while IFS= read -r line; do
      MESSAGES+=("\$line")
    done < "/tmp/newrelic-databricks-start-integration.sh.stdout.log"
  fi

  if [ "$NEW_RELIC_DATABRICKS_COPY_STARTUP_LOGS_ENABLED" = "true" ]; then
    cp /tmp/newrelic-databricks-start-integration.sh.stdout.log /dbfs/tmp/$DB_CLUSTER_ID/newrelic-databricks-start-integration.sh.stdout.log
  fi
elif [ "$NEW_RELIC_DATABRICKS_SEND_STARTUP_LOGS_ENABLED" = "true" ]; then
  MESSAGES+=("Integration startup stdout log file not found!")
fi

if [ -f /tmp/newrelic-databricks-start-integration.sh.stderr.log ]; then
  if [ "$NEW_RELIC_DATABRICKS_SEND_STARTUP_LOGS_ENABLED" = "true" ]; then
    while IFS= read -r line; do
      MESSAGES+=("\$line")
    done < "/tmp/newrelic-databricks-start-integration.sh.stderr.log"
  fi

  if [ "$NEW_RELIC_DATABRICKS_COPY_STARTUP_LOGS_ENABLED" = "true" ]; then
    cp /tmp/newrelic-databricks-start-integration.sh.stderr.log /dbfs/tmp/$DB_CLUSTER_ID/newrelic-databricks-start-integration.sh.stderr.log
  fi
elif [ "$NEW_RELIC_DATABRICKS_SEND_STARTUP_LOGS_ENABLED" = "true" ]; then
  MESSAGES+=("Integration startup stderr log file not found!")
fi

CHECKS=()

if [ -d "$NEW_RELIC_DATABRICKS_TARGET_DIR" ]; then
  CHECKS+=("Integration directory .................... PASS")
else
  CHECKS+=("Integration directory .................... FAIL")
fi

if [ -x "$NEW_RELIC_DATABRICKS_TARGET_DIR/newrelic-databricks-integration" ]; then
  CHECKS+=("Integration binary ....................... PASS")
else
  CHECKS+=("Integration binary ....................... FAIL")
fi

if [ -x "$NEW_RELIC_DATABRICKS_TARGET_DIR/start-integration.sh" ]; then
  CHECKS+=("Integration startup script ............... PASS")
else
  CHECKS+=("Integration startup script ............... FAIL")
fi

if [ -f "$NEW_RELIC_DATABRICKS_TARGET_DIR/configs/config.yml" ]; then
  CHECKS+=("Integration config file .................. PASS")
else
  CHECKS+=("Integration config file .................. FAIL")
fi

if [ -f /etc/newrelic-infra.yml ]; then
  CHECKS+=("New Relic Infrastructure enabled ......... YES")
else
  CHECKS+=("New Relic Infrastructure enabled ......... NO")
fi

if [ -f /etc/newrelic-infra/logging.d/logging.yml ]; then
  CHECKS+=("New Relic Infrastructure logs enabled .... YES")
else
  CHECKS+=("New Relic Infrastructure logs enabled .... NO")
fi

if [ "\$(ps -ef | grep '/databricks/driver/newrelic/newrelic-databricks-integration' | grep -v grep | wc -l)" -eq 1 ]; then
  CHECKS+=("Integration process running .............. YES")
else
  CHECKS+=("Integration process running .............. NO")
fi

if [ "\$(ps -ef | grep '/usr/bin/newrelic-infra' | grep -v grep | wc -l)" -eq 2 ]; then
  CHECKS+=("Infrastructure processes running ......... YES")
else
  CHECKS+=("Infrastructure processes running ......... NO")
fi

if [ "\$(ps -ef | grep '/opt/fluent-bit/bin/fluent-bit' | grep -v grep | wc -l)" -eq 1 ]; then
  CHECKS+=("Fluent Bit process running ............... YES")
else
  CHECKS+=("Fluent Bit process running ............... NO")
fi

if [ "$NEW_RELIC_DATABRICKS_SEND_STARTUP_LOGS_ENABLED" = "true" ]; then
  for check in "\${CHECKS[@]}"; do
    MESSAGES+=("\$check")
  done

  send_messages "\${MESSAGES[@]}"
fi

if [ "$NEW_RELIC_DATABRICKS_COPY_STARTUP_LOGS_ENABLED" = "true" ]; then
  touch /tmp/newrelic-databricks-integration-startup-checks.log

  for check in "\${CHECKS[@]}"; do
    echo "\$check" >> /tmp/newrelic-databricks-integration-startup-checks.log
  done

  cp /tmp/newrelic-databricks-integration-startup-checks.log /dbfs/tmp/$DB_CLUSTER_ID/newrelic-databricks-integration-startup-checks.log
fi
EOM
  chmod 500 $NEW_RELIC_DATABRICKS_TARGET_DIR/startup-helper.sh
fi

# Create the systemd service file
echo "Creating the Databricks Integration systemd service file..."

SYSTEMD_STARTUP_LOGS_CONFIG=""
if [ "$NEW_RELIC_DATABRICKS_SEND_STARTUP_LOGS_ENABLED" = "true" ] || \
   [ "$NEW_RELIC_DATABRICKS_COPY_STARTUP_LOGS_ENABLED" = "true" ]; then
  # Setup the systemd service to log to files when send or copy startup logs is
  # enabled, and to run the startup helper script to send and/or copy the
  # startup logs and checks after starting the service.
  echo "Generating the Databricks Integration service startup logs configuration..."

  SYSTEMD_STARTUP_LOGS_CONFIG="
StandardOutput=append:/tmp/newrelic-databricks-start-integration.sh.stdout.log
StandardError=append:/tmp/newrelic-databricks-start-integration.sh.stderr.log
ExecStartPost=$NEW_RELIC_DATABRICKS_TARGET_DIR/startup-helper.sh
TimeoutStartSec=5min"
fi

sudo cat <<EOM > /etc/systemd/system/newrelic-databricks-integration.service
[Unit]
Description=New Relic Databricks Integration
After=network.target

[Service]
RuntimeDirectory=newrelic-databricks-integration
WorkingDirectory=$NEW_RELIC_DATABRICKS_TARGET_DIR
Type=exec
ExecStart=$NEW_RELIC_DATABRICKS_TARGET_DIR/start-integration.sh
$SYSTEMD_STARTUP_LOGS_CONFIG
MemoryLimit=1G
# MemoryMax is only supported in systemd > 230 and replaces MemoryLimit. Some cloud dists do not have that version
# MemoryMax=1G
Restart=always
RestartSec=30
StartLimitInterval=200
StartLimitBurst=5

[Install]
WantedBy=multi-user.target
EOM

# Enable and run the service
echo "Enabling and starting the Databricks Integration service..."

sudo systemctl daemon-reload
sudo systemctl enable newrelic-databricks-integration.service
sudo systemctl start newrelic-databricks-integration

echo "Databricks Integration installation complete."
