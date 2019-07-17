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

#####################################################################
# Copy day-hour partions from storage/main grid to audience grid
#
# Two input param:
#   $1: the source HDFS directory
#   $2: the target HDFS directory
#####################################################################
function copy_to_audience_hdfs() {
  HADOOP_USER_NAME=${APP_USER} hadoop fs -rm -r -f $2
  HADOOP_USER_NAME=${APP_USER} hadoop distcp $1 $2
}

#####################################################################
# Copy day-hour partions from audience grid to SFTP staging directory
#
# Two input param:
#   $1: the source HDFS directory in the audience grid:
#       Example: /user/dmp/xdi/xdi-export/y=2015/m=01/d=01/h=01
#   $2: the date string in format "yyyyMMdd"
#       Example: "20150801"
#   $3: the hour part
#       Example: "01"
#####################################################################
function copy_to_sftp() {
  sftp_staging="${SFTP_BASE_DIR}/.staging/$2/$3"
  sftp_final="${SFTP_BASE_DIR}/$2"

  rm -rf "${sftp_staging}"
  mkdir -p "${sftp_staging}" "${sftp_final}"

  # copy to staging first: .staging/20150801/01/part-m-00000.gz
  HADOOP_USER_NAME=${APP_USER} hadoop fs -copyToLocal "$1/*.gz" "${sftp_staging}"

  # move all files from staging to final SFTP folder
  ls "${sftp_staging}" | xargs -I {} mv -f "${sftp_staging}/{}" "${sftp_final}/ox_$2_$3_{}"
  rm -rf "${sftp_staging}"
}

#####################################################################
# Main function
#
#####################################################################
function main() {
  # first run two functions to determine the range of partitions to be copied
  local JOB_START_HOUR=$(get_lock "${GET_LOCK_PATH}" "${START_LOCK_NAME}")
  local JOB_END_TIME=$(get_lock "${GET_LOCK_PATH}" "${END_LOCK_NAME}")

  # the XDI feed lock may include minutes, eg "2015-08-25 18:40:00"
  # which means its hour partition is the current working partition
  # NOT the completed hour partition defined by JOB_END_HOUR
  # 1) need to get the previous hour to avoid missing files and
  #    "path not existing error"
  # 2) reset the minute and second parts to 00
  # sample: JOB_END_HOUR="2015-08-25 17:00:00"
  local FEED_COMPLETE_HOUR=$(date -d "${JOB_END_TIME} 1 hour ago" +'%F %T')
  local JOB_END_HOUR=$(echo $(echo "${FEED_COMPLETE_HOUR}" | cut -d':' -f1)":00:00")

  # sample: 2015-01-01 00:00:00
  local working_hour="${JOB_START_HOUR}"

  echo "====================================================="
  echo "Copying Start Lock: ${JOB_START_HOUR}"
  echo "Copying End   Lock: ${JOB_END_HOUR}"
  echo "====================================================="

  while [ "${working_hour}" \< "${JOB_END_HOUR}" ]
  do
    # previous hour +1 hour in the format of 2015-01-01 01:00:00
    working_hour=$(date -d "${working_hour} 1 hour" +'%F %T')

    # extract the individual date hour parts from the datetime string
    # the year part, 2015
    # the month part 01
    # the day part 01
    # the hour part 01
    IFS=' ' read par_year par_month par_day par_hour <<< $(echo "${working_hour}" | awk -F '[- :]' '{ print $1,$2,$3,$4 }')

    # convert the working hour to HDFS path /y=2015/m=01/d=01/h=01
    local hdfs_par="/y=${par_year}/m=${par_month}/d=${par_day}/h=${par_hour}"

    # first launch distcp to copy the data from storage or main grid
    copy_to_audience_hdfs "${SRC_HDFS_BASE}${hdfs_par}" "${TARGET_HDFS_BASE}${hdfs_par}"

    local working_src_dir="${XDI_AUDIENCE_STAGING_DIR}${hdfs_par}"
    local working_target_dir="${XDI_EXPORT_DIR}${hdfs_par}"

    echo "====================================================="
    echo "copying ${working_src_dir}" to "${working_target_dir}"
    echo "====================================================="

    # only advance the lock if the query above completes successfully
    local LOCK_DATE_IN_SQL=$(convert_date_for_rest "${working_hour}")
    set_lock "${UPDATE_LOCK_PATH}" "${START_LOCK_NAME}" "${LOCK_DATE_IN_SQL}"
  done
}

# script entry point
main "$@"
