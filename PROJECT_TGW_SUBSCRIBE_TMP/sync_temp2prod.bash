#!/bin/bash
##############################################################################################################
## Script Name  : sync_temp2prod.bash                                                                              ##
## Description  : TGW Script service to sync temp data to prod
## Creator      : Cheewaphat Loy/True IT.                                                                   ##
## Created Date : 2018-02-12                                                                                ##
##############################################################################################################

CURR_DIR="$( cd "$( dirname "${0}" )" && pwd )"
CURR_USER="$(whoami)"
RUNTIME=`date "+%Y%m%d%H%M%S"`
RUNDATE=`date "+%Y%m%d"`
PID=$$

##### LOAD ENV #####
. ${CURR_DIR%/}/profile.env || source ${CURR_DIR%/}/profile.env

function init()
{    
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
    
    exec 2>&1 # show all stderr console
}

function process()
{
    echo " process"

    # get data temp PPN_TM (SYSDATE-1)

    # get data prodcution per date PPN_TM (SYSDATE-1)

    # diff record

    # insert into prod

    # summary

    
}

function clean()
{
    echo "clean"
}


#===================== MAIN =====================#
init
process
clean
exit 0 
