#!/bin/bash

# Ensure loader is available
EXE_FILE_NAME=${EXE_FILE_NAME:-$(which tsbs_load_opengemini)}
if [[ -z "$EXE_FILE_NAME" ]]; then
    echo "tsbs_load_openGemini not available. It is not specified explicitly and not found in \$PATH"
    exit 1
fi

# Load parameters - common
DATA_FILE_NAME=${DATA_FILE_NAME:-influx-data}
DATABASE_PORT=${DATABASE_PORT:-8086}
GRPC_PORT=${GRPC_PORT:-8305}


EXE_DIR=${EXE_DIR:-$(dirname $0)}
source ${EXE_DIR}/load_common.sh

until curl http://${DATABASE_HOST}:${DATABASE_PORT}/ping 2>/dev/null; do
    echo "Waiting for openGemini"
    sleep 1
done

# Remove previous database
curl -X POST http://${DATABASE_HOST}:${DATABASE_PORT}/query?q=drop%20database%20${DATABASE_NAME}
# Load new data
cat ${DATA_FILE} | gunzip | $EXE_FILE_NAME \
                                --db-name=${DATABASE_NAME} \
                                --workers=${NUM_WORKERS} \
                                --batch-size=1 \
                                --reporting-period=${REPORTING_PERIOD} \
                                --urls="http://${DATABASE_HOST}:${DATABASE_PORT},${DATABASE_HOST}:${GRPC_PORT}"
