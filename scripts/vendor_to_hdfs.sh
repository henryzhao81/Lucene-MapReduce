#!/bin/bash
export HADOOP_USER_NAME=dmp

LAST_SUCCESSED=""
LATEST_LIST=""
STATUS_FOLDER=""
STATUS_FILE=""
HDFS_FILE_FOLDER="/user/dmp/xdi/result"

function find_new_files() {
	local folder=$1
	LAST_SUCCESSED=$(defined_or_default $2 19000101)
	LATEST_LIST=`ls -1 ${folder} | sort`
}

function defined_or_default() {
	if [ -n "$1" ]
	then
		echo "$1"
	else
		echo "$2"
	fi
}

function info() {
	log "INFO" "$1"
}

function debug() {
	log "DEBUG" "$1"
}

function log() {
	# print out [LEVEL] TIME MESSAGE
	echo "[$1] `date`:" "$2"
} 

function process() {
	local date=$1
	local vendor=$2

	dirpath=$HDFS_FILE_FOLDER/$date

	# test if we have created date folder for the date
	hdfs dfs -test -d $dirpath
	if [ $? != 0 ]; then
		hdfs dfs -mkdir $dirpath
	else
		debug "Date [$date] directory already present in HDFS"
	fi
        if [ -e ${BASE_DIR}/$vendor/data/xdi-result/$date/done ] || [ -e ${BASE_DIR}/$vendor/data/xdi-result/$date/"$date"00.done ]
        then
                destination_path=$HDFS_FILE_FOLDER/$date/$vendor
                hdfs dfs -test -d $destination_path
                if [ $? != 0 ]; then
                        #if we don't have the folder then we don't upload it.
                        info "uploading $vendor-$date to HDFS"
                        hdfs dfs -mkdir $dirpath/$vendor
                        hdfs dfs -copyFromLocal ${BASE_DIR}/$vendor/data/xdi-result/$date/* $destination_path
                        hdfs dfs -touchz $destination_path/_SUCCESS
                else
                        debug "Directory already present in HDFS"
                fi
                echo $date > $3
        else
                info "skip unfinished uploading"
        fi
}

function find_vendor_new_data() {
	local vendor=$1
	info "Check new files for [$vendor]"
	status_file=$STATUS_FOLDER/$vendor.done
	status=`cat $status_file`
	find_new_files ${BASE_DIR}/$vendor/data/xdi-result ${status}
	for i in $LATEST_LIST; do
		if [[ "$i" > "$LAST_SUCCESSED" ]];
		then
			process $i $vendor $status_file
		else
			debug "Skip $vendor-$i, already processed"
		fi
	done
}

function main() {
	BASE_DIR=$1
	STATUS_FOLDER=$2
	find_vendor_new_data $3
}

main $1 $2 $3
