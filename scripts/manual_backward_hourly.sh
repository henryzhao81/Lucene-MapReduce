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

#####################################################################
# Copy day-hour partions from storage/main grid to audience grid
#
# Two input param:
#   $1: the source HDFS directory
#   $2: the target HDFS directory
#####################################################################
function copy_to_audience_hdfs() {
  HADOOP_USER_NAME=${APP_USER} hadoop fs -rm -f -r $2
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
  local JOB_START_HOUR="$1"
  local JOB_END_HOUR="$2"
  #JOB_END_HOUR="2015-08-25 18:00:00"

  # sample: 2015-01-01 00:00:00
  local working_hour="${JOB_END_HOUR}"

  echo "====================================================="
  echo "Copying Start Lock: ${JOB_START_HOUR}"
  echo "Copying End   Lock: ${JOB_END_HOUR}"
  echo "====================================================="

  while [ "${working_hour}" \> "${JOB_START_HOUR}" ]
  do
    # previous hour +1 hour in the format of 2015-01-01 01:00:00
    working_hour=$(date -d "${working_hour} 1 hour ago" +'%F %T')

    # extract the individual date hour parts from the datetime string
    # the year part, 2015
    # the month part 01
    # the day part 01
    # the hour part 01
    read par_year par_month par_day par_hour <<< $(echo "${working_hour}" | awk -F '[- :]' '{ print $1,$2,$3,$4 }')

    # convert the working hour to HDFS path /y=2015/m=01/d=01/h=01
    local hdfs_par="/y=${par_year}/m=${par_month}/d=${par_day}/h=${par_hour}"

    # first launch distcp to copy the data from storage or main grid
    copy_to_audience_hdfs "${SRC_HDFS_BASE}${hdfs_par}" "${TARGET_HDFS_BASE}${hdfs_par}"

    local working_src_dir="${XDI_AUDIENCE_STAGING_DIR}${hdfs_par}"
    local working_target_dir="${XDI_EXPORT_DIR}${hdfs_par}"

    echo "====================================================="
    echo "copying ${working_src_dir}" to "${working_target_dir}"
    echo "====================================================="

    # call a Pig script to filter publishers and produce the export files
    HADOOP_USER_NAME=${APP_USER} pig -p "INPUT_DIR=${working_src_dir}" -p "OUTPUT_DIR=${working_target_dir}" -p "JAR_FILE=${XDI_HOME}/lib/xdi-audience-grid-${XDI_VERSION}-with-dependencies.jar" -p "OUTPUT_FILE_SIZE=${EXPORT_FILE_SIZE}" "${XDI_HOME}/lib/xdi_file_gen.pig"

    # copy file from HDFS to SFTP staging directory
    copy_to_sftp "${working_target_dir}" "${par_year}${par_month}${par_day}" "${par_hour}"
  done
}

# script entry point
if [ $# -lt 2 ]; then
  echo "Usage: to copy all hours for 2015/09/22, use the following parameters"
  echo "$0 \"2015-09-22 00:00:00\" \"2015-09-23 00:00:00\""
  exit 1
fi

main "$@"
