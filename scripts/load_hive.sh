
#!/bin/bash

export HADOOP_USER_NAME=dmp

status_aud=$(hadoop fs -cat hdfs://xvaic-sg-36.xv.dc.openx.org:8020/user/dmp/adroll/sync.status)
echo "AUD Status : $status_aud"

status_storage=$(hadoop fs -cat /user/dmp/cookie_mapping_status/sync.status)
echo "STORAGE Status : $status_storage"

#next_year=$(date +%Y -d "$status_aud + 1 day")
year=$(date +%Y -d "$status_aud - 1 day")
month=$(date +%m -d "$status_aud - 1 day")
day=$(date +%d -d "$status_aud - 1 day")
target_aud=$year$month$day
echo "date : $target_aud"

date_aud=$(expr "$target_aud" + "0")
date_storage=$(expr "$status_storage" + "0")
echo "aud : $date_aud storage : $date_storage"

LOCK=/var/tmp/sync_lock

if [ -f $LOCK ]; then
    echo "job still running, exit"
    exit 6
fi

if [ "$date_aud"  -gt "$date_storage" ]; then
    echo "start lock"
    touch $LOCK
    echo "alter table cookie_mapping add if not exists partition (y='$year', m='$month', d='$day');"
    hive -e "alter table cookie_mapping add if not exists partition (y='$year', m='$month', d='$day');"
    if hadoop fs -test -d /user/dmp/cookie_mapping_ext/y=$year/m=$month/d=$day ; then
        echo "hadoop distcp hdfs://xvaic-sg-36.xv.dc.openx.org:8020/user/dmp/adroll/pixel/$target_aud/part-r-* hdfs:///user/dmp/cookie_mapping_ext/y=$year/m=$month/d=$day"
        hadoop distcp hdfs://xvaic-sg-36.xv.dc.openx.org:8020/user/dmp/adroll/pixel/$target_aud/part-r-* hdfs:///user/dmp/cookie_mapping_ext/y=$year/m=$month/d=$day
        echo "sync finished, delete /user/dmp/cookie_mapping_status/sync.status"
        hadoop fs -rm -r /user/dmp/cookie_mapping_status/sync.status
        echo "add new data $target_aud"
        echo "$target_aud" | hadoop fs -put - /user/dmp/cookie_mapping_status/sync.status
    else
        echo "failed to create partition $year $month $day"
    fi
    echo "delete lock"
    rm $LOCK
else
    echo "Nothing happen!"
fi
