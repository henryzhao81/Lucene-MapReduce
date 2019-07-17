#!/bin/bash

# the XDI product version number defined in the pom.xml
# this is necessary because the Pig script xdi_file_gen needs to register
# this jar file to use the custom load function
XDI_VERSION="@PACKAGE_VERSION@"

# the user who will own all the files and processes
APP_USER="dmp"

#########################################################################
# the source grid where the ad requests are copied from
#########################################################################
# main grid name node
SRC_GRID_NAMENODE="hdfs://prod-cdh-grid-namenode1.xv.dc.openx.org:8020"

# the XDI feed location
XDI_SRC_BASE_DIR="/grid/ox3/eventlogs/xdi/v=2"

# the fully qualified HDFS path for storage grid staging table
SRC_HDFS_BASE="${SRC_GRID_NAMENODE}${XDI_SRC_BASE_DIR}"

#########################################################################
# the target grid where the ad requests are copied to
#########################################################################
# audience grid name node
TARGET_GRID_NAMENODE="hdfs://xvaic-sg-36.xv.dc.openx.org:8020"

# the base dir on the audience grid
XDI_AUDIENCE_BASE_DIR="/user/${APP_USER}/xdi"

# the XDI internal staging table on the audience grid
XDI_AUDIENCE_STAGING_DIR="${XDI_AUDIENCE_BASE_DIR}/xdi-market-request"

# the XDI export table on the audience grid
XDI_EXPORT_DIR="${XDI_AUDIENCE_BASE_DIR}/xdi-export"

# the fully qualified HDFS path for audience grid staging table
TARGET_HDFS_BASE="${TARGET_GRID_NAMENODE}${XDI_AUDIENCE_STAGING_DIR}"

# the top level SFTP directory to store OpenX export files
SFTP_BASE_DIR="/var/mnt/mfs/services/eds-user-segment-sftp/openx-xdi"

# the latest HFile directory pattern (ready to be loaded to HBase cluster)
AG_LATEST_HFILE_DIR="/user/dmp/xdi/hfile/*/_SUCCESS"

# Vendor files uploading statue directory
VENDOR_STATUS_DIR="/var/log/xdi-audience-grid/vendor_status"

# the desired export file size in bytes before compression is 1.5 GB
EXPORT_FILE_SIZE=1610612736

#########################################################################
# the grid Lock API constants
#########################################################################
# Grid Lock REST API endpoint
LOCK_REST_BASE_URL="http://prod-cdh-grid-locks-rest.xv.dc.openx.org:8080"
# the start and end lock names
START_LOCK_NAME="AG_XDI_EXPORT_COMPLETED"
END_LOCK_NAME="XDI_AUDIENCE_INFERENCE_FEED"

# the end point for getting a lock timestamp by name
GET_LOCK_PATH="${LOCK_REST_BASE_URL}/locks/query"
# the end point for updating the lock
UPDATE_LOCK_PATH="${LOCK_REST_BASE_URL}/locks/update"

#########################################################################
# the Mondemand configuration file
#########################################################################
MONDEMAND_CONF_FILE="/etc/mondemand/mondemand.conf"


# if the global env configuration file is present, the environment parameters
# there will take precedence
ENV_CONF_FILE="/etc/xdi-audience-grid/env_override.sh"
[ -f "${ENV_CONF_FILE}" ] && source "${ENV_CONF_FILE}"

