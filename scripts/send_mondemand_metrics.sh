#!/bin/bash

# Explanation of the bash options
# -e: exit immediately if any command exits with a non-zero code
# -u: exit if variables are not initialized
# -o pipefail: the whole pipeline returns non-zero code if any command
#              fails (default the last command)
set -euo pipefail
IFS=$'\n\t'

XDI_HOME=$( cd "$( dirname "${BASH_SOURCE[0]}" )/.." && pwd )

# import all XDI configured parameters
source "${XDI_HOME}/conf/xdi_env.sh"

# import the grid lock related constants and functions
# get_lock()
# set_lock()
# convert_date_for_rest()
#
source "${XDI_HOME}/lib/lock_util.sh"

# set the Mondemand server address and port
source "${MONDEMAND_CONF_FILE}"

#####################################################################
# Send pipeline related metrics to Mondemand server
# $1 - main XDI feed lagging in hours
# $2 - audience grid sftp job lagging in hours
# $3 - vendor result files loading lagging in hours
# $4 - XDI HFile loading lagging in hours
#####################################################################
function metrics_to_mondemand () {
  mondemand-tool -o lwes::${MONDEMAND_ADDR}:${MONDEMAND_PORT} \
    -p xdi_pipeline \
    -c host:`hostname -s` \
    -s gauge:main_feed_lag_sec:"$1" \
    -s gauge:ag_sftp_lag_sec:"$2" \
    -s gauge:ag_vendor_load_lag_sec:"$3" \
    -s gauge:ag_hbase_lag_sec:"$4"
}

#####################################################################
# Get the latest vendor status date from a status file
# $1 - vendor ID. For example, "screen6"
#
# Output sample: 20151009
#####################################################################
function get_vendor_latest_date () {
  cat "${VENDOR_STATUS_DIR}/$1.done"
}


#####################################################################
# Return the latest date in 'yyyyMMdd' format by which the Hfiles
# are loaded into HBase for serving
#
# Output sample: 20151009
#####################################################################
function get_latest_hfile_date() {
  echo $(HADOOP_USER_NAME=${APP_USER} hadoop fs -ls "${AG_LATEST_HFILE_DIR}"  | tail -n 1 | cut -d'/' -f6)
}


#####################################################################
# Main function
#
#####################################################################
function main() {
  # first run two functions to determine the range of partitions to be copied
  local MAIN_LOCK_TIME=$(get_lock "${GET_LOCK_PATH}" "${END_LOCK_NAME}")
  local AG_LOCK_TIME=$(get_lock "${GET_LOCK_PATH}" "${START_LOCK_NAME}")
  local S6_LATEST_DATE=$(get_vendor_latest_date "screen6")
  local HFILE_LATEST_DATE=$(get_latest_hfile_date)

  local MAIN_SEC=$(date -u -d "${MAIN_LOCK_TIME}" "+%s")
  local SFTP_SEC=$(date -u -d "${AG_LOCK_TIME}" "+%s")
  local S6_SEC=$(date -u -d "${S6_LATEST_DATE} +1 day" "+%s")
  local HFILE_SEC=$(date -u -d "${HFILE_LATEST_DATE} +1 day" "+%s")

  local NOW_SEC=$(date "+%s")

  local MAIN_LAG_SEC=$(echo "${NOW_SEC} - ${MAIN_SEC}" | bc)
  local SFTP_LAG_SEC=$(echo "${MAIN_SEC} - ${SFTP_SEC}" | bc)
  local S6_LAG_SEC=$(echo "${NOW_SEC} - ${S6_SEC}" | bc)
  local HFILE_LAG_SEC=$(echo "${NOW_SEC} - ${HFILE_SEC}" | bc)

  metrics_to_mondemand "${MAIN_LAG_SEC}" "${SFTP_LAG_SEC}" "${S6_LAG_SEC}" "${HFILE_LAG_SEC}"
}

# script entry point
main "$@"
