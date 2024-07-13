#!/bin/bash
#java -cp ".:mysql-connector.jar:org.json.jar" StartThread
cd /opt/DR_CRON/DR-LoadShift-Live
java -cp "/opt/DR_CRON/DR-LoadShift-Live/:mysql-connector.jar:org.json.jar" StartThread
