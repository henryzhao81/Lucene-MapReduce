#!/bin/bash

# Explanation of the bash options
# -e: exit immediately if any command exits with a non-zero code
# -u: exit if variables are not initialized
# -o pipefail: the whole pipeline returns non-zero code if any command
#              fails (default the last command)
set -euo pipefail
IFS=$'\n\t'

#####################################################################
# Get the date hour partition that the job finished copying in the
# previous run.
#
# Input:
#   $1 - get lock API URL
#   $2 - lock name, eg "SG_XDI_COPY_COMPLETED"
#
# Output:
#   2015-01-01 00:00:00 (sample)
#
# Note: The new run should start from +1 hour.
#
#####################################################################
function get_lock() {
  echo $(curl "$1/$2" --stderr - | grep -oPm1 "(?<=<time>)[^<]+" | tr 'TZ' ' ')
}

#####################################################################
# Set the XDI copy lock after completing an hour partition
#
# Input:
#  $1 - update lock API URL
#  $2 - Lock name, eg "SG_XDI_COPY_COMPLETED"
#  $3 - Lock timestamp, eg "2015-08-26T12:00:00Z"
#
#####################################################################
function set_lock() {
  echo "setting Lock [$2] to this hour [$3]"
  curl -X POST -H "Content-type: application/xml"  -d "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?><Lock><name>$2</name><time>$3</time></Lock>" $1
}

#####################################################################
# Convert the UTC date time to the java.sql.Timestamp required by
# the Grid Lock REST API.
#
# Input:
#  $1 - 2015-08-26 12:00:00 (date hour in UTC format)
#
# Output:
#  2015-08-26T12:00:00Z (date hour formatted in sql Timestamp format)
#
#####################################################################
function convert_date_for_rest() {
  tmp=$(echo "$1" | tr ' ' 'T')
  echo "${tmp}Z"
}

