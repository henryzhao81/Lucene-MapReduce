#!/bin/bash
CURR_DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )

ps -ef | grep bash | grep xdi_process_adrequest
if [ $? == 0 ]; then
	echo $(date +"%F %H:%M:%S") "Previous copying is still running so will not start this job."
	exit 1
fi

bash "${CURR_DIR}/xdi_process_adrequest.sh"