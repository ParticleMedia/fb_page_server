#!/bin/bash -e
cd $(dirname $0)

LOG_DIR=../log
CONF_PATH=../conf/fb_page_tcat.yaml
LOG_LEVEL=3
ALSO_LOG_TO_STDERR=true
PUSH_TSDB=false

if [ "x"$1 == "xdebug" ]; then
    CONF_PATH=../conf/nonlocal_indexer_debug.yaml
    LOG_LEVEL=16
    ALSO_LOG_TO_STDERR=true
    PUSH_TSDB=false
elif [ "x"$1 == "xk8s" ]; then
    ALSO_LOG_TO_STDERR=true
fi

mkdir -p ${LOG_DIR}
echo $$ >./pid
exec ./fb_page_tcat -conf=${CONF_PATH} \
    -log_dir=${LOG_DIR} \
    -v=${LOG_LEVEL} \
    -alsologtostderr=${ALSO_LOG_TO_STDERR} \
    -metrics_to_tsdb=${PUSH_TSDB}
rm -f ./pid
