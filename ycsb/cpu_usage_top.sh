#!/bin/bash
PNAME="$1"
LOG_FILE="$2"
echo "$PNAME"
PID=$(/usr/sbin/pidof ${PNAME})
while [[ -z $PID ]]; do
  PID=$(/usr/sbin/pidof "${PNAME}")
  sleep 1
done
top -b -d 1 -p $PID | awk \
    -v cpuLog="$LOG_FILE" -v pid="$PID" -v pname="$PNAME" '
    /^top -/{time = $3}
    $1+0>0 {printf "%s %s :: %s[%s] CPU Usage: %d%%\n", \
            strftime("%Y-%m-%d"), time, pname, pid, $9 > cpuLog
            fflush(cpuLog)}'