#!/usr/bin/env bash

# intended to be started via cron every 1 minute on one node in cluster,
#  via isi_ropc (aka "run once per cluster"),
#  to look for $LOOK_FOR process running and current heartbeat

# If no process found or no heartbeat or too many processes
#  it is supposed to right the situation...

# To implement, create or append cron entry in to
#  /etc/mcp/override/crontab.smbtime
# Remove pound sign on line with command.
# Ensure CopyService's phase1 script is up and running at all times
#*     *       *       *       *       root    isi_ropc /ifs/copy_svc/check_running.sh 2> /dev/null


CONFIG_FILE="/ifs/copy_svc/config"

LOOK_FOR=$(grep -A5 "\[Check_running_one_node\]" ${CONFIG_FILE} \
          |grep ^Process \
          |awk -F":  " '{print$2}')

PARENT_LOG_PATH=$(grep -A5 "\[Check_running_one_node\]" ${CONFIG_FILE} \
          |grep ^LogPath \
          |awk -F":  " '{print$2}')
LOG_FILENAME=$(grep -A5 "\[Check_running_one_node\]" ${CONFIG_FILE} \
          |grep ^LogFile \
          |awk -F":  " '{print$2}')
PATH_TO_LOG="${PARENT_LOG_PATH}/${LOG_FILENAME}"

STALE_HEARTBEAT=$(grep -A5 "\[Check_running_one_node\]" ${CONFIG_FILE} \
          |grep ^Stale \
          |awk -F":  " '{print$2}')

ROTATE_FREQUENCY=$(grep -A5 "\[Check_running_one_node\]" ${CONFIG_FILE} \
          |grep ^Rotate \
          |awk -F":  " '{print$2}')

#echo $LOOK_FOR
#echo $PARENT_LOG_PATH
#echo $PATH_TO_LOG
#echo $STALE_HEARTBEAT
#echo $ROTATE_FREQUENCY
#exit

# Since heartbeats are stored in UTC, the -u option is used here
function get_date() {
    date -u +'%Y, %m, %d, %H, %M, %S'
}

function rotate_log() {
    if [[ $(date +'%H%M') = '0000' ]]
        then
            log_line "Rolling log file now."
            SHORT_DATE=$(date +%Y%m%d)
            NEW_LOG=${PATH_TO_LOG}.${SHORT_DATE}
            cp $PATH_TO_LOG $NEW_LOG
            cat /dev/null > $PATH_TO_LOG
            log_line "Starting new log file."

            find $PARENT_LOG_PATH -regex ".*$PATH_TO_LOG\.[[:digit:]]*" \
                -mtime +${ROTATE_FREQUENCY} -exec gzip -f9 {} \;
    fi
}

function log_line() {
    _RIGHT_NOW=$(get_date)
    echo "$_RIGHT_NOW : $1" >> $PATH_TO_LOG
}

function get_running() {
    _A_TEMP_RUNNING=$(isi_for_array -X ps auxwww \
                    |grep $LOOK_FOR |grep -v grep \
                    |grep -v "sh -c /usr/bin/python $LOOK_FOR" \
                    |grep -v isi_rdo \
                    |grep -v isi_for_array \
                    |grep -v ropc)
    echo "$_A_TEMP_RUNNING"
}

function get_frequency() {
    _B_TEMP_RUNNING="$1"
    _A_FREQUENCY=$(echo "$_B_TEMP_RUNNING" | wc -l)
    _B_FREQUENCY=${_A_FREQUENCY//[[:space:]]/}
    echo "$_B_FREQUENCY"
}

function get_last_heartbeat() {
    _A_TIMESTAMP=$(more /ifs/copy_svc/find_dirs_hb.dat \
                  |awk -F", " '{print$1, $2, $3, $4, $5, $6}')    
# Create stale entry to test action
#    _A_TIMESTAMP=$(more /ifs/copy_svc/_find_dirs_hb.dat \
#                  |awk -F", " '{print$1, $2, $3, $4, $5, $6}')    
    echo "$_A_TIMESTAMP"
}

function get_heartbeat_diff() {
    _B_TIMESTAMP=$(get_last_heartbeat)
    __HB=`date -jf "%Y %m %d %H %M %S" "${_B_TIMESTAMP}" +%s`

    # Here -u option doesn't return in UTC so adding 7 hours to now
    __NOW=$(date -v+7H +%s)

    __DIFF=$(expr $__NOW - $__HB)
    echo "$__DIFF"
}

function test_ok() {
    _RUNNING="$1"
    _FREQUENCY="$2"
    _LAST_HEARTBEAT="$3"
    _LAST_HEARTBEAT_DIFF="$4"
    if [[ ! -z ${_RUNNING} ]] \
    && [[ ${_FREQUENCY} -eq 1 ]] \
    && [[ $_LAST_HEARTBEAT_DIFF -lt $STALE_HEARTBEAT ]]
        then
            log_line "Found $LOOK_FOR with current heartbeat $_LAST_HEARTBEAT" 
            log_line "$_RUNNING"
            exit 1
    fi
}

function kill_all() {
    /usr/bin/isi_for_array /bin/pkill "python $LOOK_FOR"
}

function start_process() {
    log_line "Starting $LOOK_FOR now"
    /usr/bin/isi_ropc /usr/bin/python $LOOK_FOR & disown
    return 1
}

# Begin work here

rotate_log
RUNNING=$(get_running)
FREQUENCY=$(get_frequency "$RUNNING")
LAST_HEARTBEAT=$(get_last_heartbeat)
LAST_HEARTBEAT_DIFF=$(get_heartbeat_diff)

#echo "running....$RUNNING"
#echo "frequency..$FREQUENCY"
#echo "last hb....$LAST_HEARTBEAT"
#echo "hb diff....$LAST_HEARTBEAT_DIFF"

test_ok "$RUNNING" "$FREQUENCY" "$LAST_HEARTBEAT" "$LAST_HEARTBEAT_DIFF"

if [[ -z ${RUNNING} ]]
    then
        log_line "$LOOK_FOR not found anywhere.  Starting now..."
elif [[ $FREQUENCY -gt 1 ]]
    then
        log_line "Too many $LOOK_FOR running."
        log_line "PS output ---> $RUNNING < ---"
        log_line "Killing all processes across the cluster."
        kill_all
elif [[ $LAST_HEARTBEAT_DIFF -gt $STALE_HEARTBEAT ]]
    then
        log_line "$LOOK_FOR running with stale heartbeat $LAST_HEARTBEAT"
        log_line "PS output ---> $RUNNING <---"

        # Resolve hostname returned to LNN (Logical Node Number)
        RUNNING_ON=$(echo $RUNNING | awk -F": root" '{print$1}')
        LNN=${RUNNING_ON##*-}

        log_line "Killing $LOOK_FOR on $RUNNING_ON now"
        /usr/bin/isi_for_array -n $LNN /bin/pkill \
            "python $LOOK_FOR"
else
    # Do not know what the heck is going on
    log_line "Something is unexpected."
    log_line "PS output ---> $RUNNING <---"
    log_line "Last heartbeat:  $LAST_HEARTBEAT"
fi

start_process
RUNNINGx2=$(get_running)
log_line "PS output ---> $RUNNINGx2 <---"
exit 1
