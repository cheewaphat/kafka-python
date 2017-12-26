#!/bin/bash
##############################################################################################################
## Script Name  : service.bash                                                                              ##
## Description  : TGW Script service to run consumer applicatiopn                                           ##
## Creator      : Cheewaphat Loy/True IT.                                                                   ##
## Created Date : 2017-12-04                                                                                ##
##############################################################################################################

CURR_DIR="$( cd "$( dirname "${0}" )" && pwd )"
CURR_USER="$(whoami)"
# PATH_TMP="${CURR_DIR%/}/temp/"
# PATH_LOG="${CURR_DIR%/}/log/"
# PATH_CFG="${CURR_DIR%/}/config/"
APP_PY="${CURR_DIR%/}/main.py"
APP_TOPIC_PY="${CURR_DIR%/}/topics.py"
RUNTIME=`date "+%Y%m%d%H%M%S"`
RUNDATE=`date "+%Y%m%d"`
PID=$$
LOG_NAME="TGW_KAFKA.log"

##### LOAD ENV #####
. ${CURR_DIR%/}/profile.env || source ${CURR_DIR%/}/profile.env

function init()
{
    #export to env path for main.py APP
    export PATH_TMP
    export PATH_LOG

    mkdir -p "${PATH_LOG%/}"
    mkdir -p "${PATH_TMP%/}"

    F_LOG="${PATH_LOG%/}/${LOG_NAME}"

    #load lib log  && write file      
    {
        source "${CURR_DIR%/}/lib/logging.bash" || . "${CURR_DIR%/}/lib/logging.bash" 
    }
    if [ $? -eq 1 ] ; then
        echo "can't load ${CURR_DIR%/}/lib/logging.bash"
    fi
    exec 3>>"${F_LOG}"
    exec 3>&1 # show all stderr console

}

function status()
{
    ps -eo pid,args | grep $APP_PY | grep -v grep | awk '{print $5}' | uniq
}

function stop()
{
    ps -eo pid,args | grep $APP_PY | grep -v grep | awk '{print $1}' | xargs kill -9 >/dev/null 2>&1
    rm -r "${CURR_DIR%/}/*.out" 2>/dev/null
    rm ${F_LOG} 2>/dev/null
}

function repair()
{
    local topic=$1    
    local _cfg="${CURR_DIR%/}/config/"
    local _pid="${CURR_DIR%/}/repair_all-${PID}.out"

    if [ ! -f "${_cfg%/}/topic_${topic%.*}.ini" ] ;then
        log_error "No topic configureion ,please check file ${_cfg%/}/topic_${topic%.*}.ini"
        exit 1
    fi

    # cmd="python ${CURR_DIR%/}/$APP_PY  -c ${_cfg%/}/topic_${topic%.*}.ini -t /tmp/workspace/TGW_MSISDN/ -l /tmp/workspace/TGW_MSISDN/ -m earliest"
    cmd="$CMD_PYTHON $APP_PY  -c ${_cfg%/}/topic_${topic%.*}.ini -m earliest --log_name ${LOG_NAME%.*}_${topic%.*}.log"
    eval "${cmd}"
    RUN_PID=$!
    log_inf "Repair topic [${RUN_PID}] : ${topic}"
    echo $RUN_PID>>"${_pid}"

    # kill ps
    sleep 299
    kill -9 `cat "${_pid}"` && rm "${_pid}"
}

function repair_all()
{

    local _cfg="${CURR_DIR%/}/config/"
    local _tmpout="${CURR_DIR%/}/.found-cfg-${PID}.out"
    local _pid="${CURR_DIR%/}/repair_all-${PID}.out"
    ls ${_cfg%/}/*.ini > "${_tmpout}"
    log_inf ""
    log_inf "lookup *.ini in ${_cfg%/}"    
    log_inf "found `wc -l "$_tmpout"`"
    
    while IFS='' read -r line || [[ -n "$line" ]]; do
        filename=$(basename "$line")        
        extension="${filename##*.}"        
        cmd="$CMD_PYTHON $APP_PY  -c ${_cfg%/}/${filename%.*}.ini -m earliest --log_name ${LOG_NAME%.*}_${filename%.*}.log"

        #check process	    
        if [ `ps -ef | grep "${_cfg%/}/${filename%.*}.ini -m earliest" | grep -v grep|  wc -l` -ne 0 ] ;then 
            log_inf "Process is still running , $cmd"
            continue 
        fi
        
        if [[ -f "${_cfg%/}/$filename" ]] ; then            
            eval "$cmd &" 
            pid=$!
            log_inf " Call :PID:$pid:$cmd"
            echo $pid>>"${_pid}"
            sleep 1
        else 
            log_inf " file format invalide"
        fi
        
    done < "$_tmpout"
    rm "${_tmpout}"

    #clear temp
    remove_archive

    # kill ps
    sleep 10
    kill -9 `cat "${_pid}"` && rm "${_pid}"

}

function topic_lists()
{
    echo "`date "+%F %T"` Test IP [ $1 ]"
    echo ""
    cmd="$CMD_PYTHON $APP_TOPIC_PY  -ip $1 >> ${PATH_TMP%/}/topics.$pid.out"   
    eval "$cmd"
    RUN_PID=$?
    cat ${PATH_TMP%/}/topics.$pid.out | sort     
    sleep 1
    rm ${PATH_TMP%/}/topics.$pid.out
    kill -9 $RUN_PID > /dev/null
}

function start()
{

    local _cfg="${CURR_DIR%/}/config/"
    local _tmpout="${CURR_DIR%/}/.found-cfg-${PID}.out"
    ls ${_cfg%/}/*.ini > "${_tmpout}"
    log_inf ""
    log_inf "lookup *.ini in ${_cfg%/}"    
    log_inf "found `wc -l "$_tmpout"`"
    
    while IFS='' read -r line || [[ -n "$line" ]]; do
        filename=$(basename "$line")        
        extension="${filename##*.}"        
        cmd="$CMD_PYTHON $APP_PY  -c ${_cfg%/}/${filename%.*}.ini --log_name ${LOG_NAME%.*}_${filename%.*}.log"

        #check process	    
        if [ `ps -ef | grep "${_cfg%/}/${filename%.*}.ini" | grep -v grep|  wc -l` -ne 0 ] ;then 
            log_inf "Process is still running , $cmd"
            continue 
        fi
        
        if [[ -f "${_cfg%/}/$filename" ]] ; then            
            eval "$cmd &" 
            pid=$!
            log_inf "Call :PID:$pid:$cmd"
            sleep 1
        else 
            log_inf "file format invalide"
        fi
        
    done < "$_tmpout"
    rm "${_tmpout}"

    #clear temp
    remove_archive
}

function remove_archive()
{
    log_inf "check remove archive on ${PATH_TMP%/}"    
    find "${PATH_TMP%/}" -type d -mtime +7 -exec rm -rf {} \; -print 2>/dev/null  
}

function move_log()
{
    # automate move log
    local _po="${CURR_DIR%/}/.point"

    if [ ! -f "${_po}" ];then
        echo $RUNDATE > "${_po}"
    fi

    if [ `cat ${_po}` != "${RUNDATE}" ];then
        mv "${F_LOG}" "${PATH_LOG%/}/${LOG_NAME%.*}_`date "+%Y%m%d" -d"-1 day"`_${PID}.log"    
    fi
    
}


#===================== MAIN =====================#
init
# move_log

case "$1" in 
    start)
       start
        ;;
    stop)
       stop
        ;;
    restart)
       stop
       start
        ;;
    status)
       status
        ;;
    repair)
        if [ -z $2 ];then
            echo "press enter \"topic\""
            exit 1
        fi

        if [ ${2,,} == "all" ]; then
            repair_all $2
        else
            repair $2
        fi

        
        ;;
    topics)
        if [ -z $2 ];then
            echo "press enter \"server {IP}:{PORT}\""
            exit 1
        fi

        topic_lists ${2,,}
        ;;
    *)
        modes=("start stop status restart repair\t{TOPIC} topics\t{IP}:{PORT}")
        echo "Usage: $0 {start|stop|status|restart|repair|topics}"
        echo "Example..."    
        for m in $modes;do
            echo -e "\t$0 $m"     
        done           
        sleep 0.5
        exit 0;
esac
exit 0 