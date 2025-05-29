#!/bin/bash

export SPARK_HOME=/opt/spark
export PATH="${SPARK_HOME}/sbin:${SPARK_HOME}/bin:${PATH}"

export JAVA_HOME=/usr/local/openjdk-11
export PATH="${JAVA_HOME}/bin:${PATH}"

export PYSPARK_PYTHON=${PYSPARK_PYTHON:-python3}
export PYSPARK_DRIVER_PYTHON=${PYSPARK_DRIVER_PYTHON:-python3}

# Logging setup is already handled in Dockerfile, so skip redundant creation
# Ensure log directory exists
LOG_DIR="${SPARK_LOG_DIR:-/opt/spark/logs}"
mkdir -p "${LOG_DIR}"

check_error() {
  if [ $? -ne 0 ]; then
    echo "Error: $1"
    exit 1
  fi
}

case "${SPARK_WORKLOAD:-undefined}" in
  "master")
    export SPARK_MASTER_HOST=$(hostname)
    echo "Starting Spark Master on ${SPARK_MASTER_HOST}:${SPARK_MASTER_PORT}"
    exec "${SPARK_HOME}/bin/spark-class" org.apache.spark.deploy.master.Master \
      --host "${SPARK_MASTER_HOST}" \
      --port "${SPARK_MASTER_PORT}" \
      --webui-port "${SPARK_MASTER_WEBUI_PORT}"
    check_error "Failed to start Spark Master"
    ;;
  "worker")
    echo "Starting Spark Worker connected to ${SPARK_MASTER}"
    exec "${SPARK_HOME}/bin/spark-class" org.apache.spark.deploy.worker.Worker \
      --webui-port "${SPARK_WORKER_WEBUI_PORT}" \
      "${SPARK_MASTER}"
    check_error "Failed to start Spark Worker"
    ;;
  "submit")
    echo "SPARK SUBMIT mode - please run spark-submit commands manually."
    exec /bin/bash
    ;;
  *)
    echo "Undefined Workload Type '${SPARK_WORKLOAD:-undefined}', must specify one of: master, worker, submit"
    exit 1
    ;;
esac