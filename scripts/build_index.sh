#!/bin/bash

ps -ef | grep java | grep IndexBuilder
if [ $? == 0 ]; then
        echo $(date +"%F %H:%M:%S") "Previous job is still running so will not start this job."
        exit 1
fi

echo $(date +"%F %H:%M:%S") "No previous job running,  start this job."

export HADOOP_USER_NAME=dmp

#java -cp "indexBuilder.jar:./lib/*" com.openx.dmp.lucene.IndexBuilder

set JAVA_OPTS="-Xms1g -Xmx18g"

java -Xms1g -Xmx16g -Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.port=9010 -Dcom.sun.management.jmxremote.local.only=false -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false -cp "builder.jar:./lib/:./conf/" com.openx.audience.xdi.lucene.IndexBuilder

