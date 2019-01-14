#!/bin/bash
_now=$(date +"%Y%m%d")
_logFile="./logs/browser_$_now.log"
_errFile="./logs/browser_$_now.err"

echo "Starting logging to $_logFile, $_errFile ..."
mkdir -p ./logs

java -jar agensbrowser-web-2.0.jar --spring.config.name=agens-config 1>$_logFile 2>$_errFile &