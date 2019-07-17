#!/bin/bash

ps -ef | grep java | grep xdiReport
if [ $? == 0 ]; then
        echo $(date +"%F %H:%M:%S") "Previous job is still running so will not start this job."
        exit 1
fi

echo $(date +"%F %H:%M:%S") "No previous job running,  start this job."

XDI_HOME=$( cd "$( dirname "${BASH_SOURCE[0]}" )/.." && pwd )

# import all XDI configured parameters
source "${XDI_HOME}/conf/xdi_env.sh"

HADOOP_USER_NAME=${APP_USER} hadoop jar ${XDI_HOME}/lib/xdi-audience-grid-${XDI_VERSION}-with-dependencies.jar com.openx.audience.xdi.xdiReport.XdiReportDriver
