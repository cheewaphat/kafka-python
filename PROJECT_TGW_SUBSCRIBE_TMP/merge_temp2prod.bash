#!/bin/bash
##############################################################################################################
## Script Name  : merge_temp2prod.bash                                                                              ##
## Description  : TGW Script service to sync temp data to prod
## Creator      : Cheewaphat Loy/True IT.                                                                   ##
## Created Date : 2018-02-12                                                                         ##
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

function dump_diff_data()
{
    F_SPOOL_RUN="${PATH_TMP%/}/exp_spool_scipt_${PID}.run"
    F_SPOOL_DAT="${PATH_TMP%/}/exp_spool_scipt_${PID}.dat"
    cat << EOF
    set colsep ,     -- separate columns with a comma
    set pagesize 0   -- No header rows
    set trimspool on -- remove trailing blanks
    set headsep off  -- this may or may not be useful...depends on your headings.
    set linesize X   -- X should be the sum of the column widths
    set numw X       -- X should be the length you want for numbers (avoid scientific notation on IDs)

    spool $F_SPOOL_DAT

    select table_name, tablespace_name 
    from all_tables
    where owner = 'SYS'
    and tablespace_name is not null;
EOF
    >> $F_SPOOL_RUN
    cat $F_SPOOL_RUN
}

function process()
{
    echo " process"

    # get data temp PPN_TM (SYSDATE-1)
    dump_diff_data

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
