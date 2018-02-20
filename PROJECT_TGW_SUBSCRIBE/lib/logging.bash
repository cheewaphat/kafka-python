#!/bin/bash
###########        Info        ###########
## Script Name  :  logging.bash
## Description  :  process logs for noting that inf() and debug() should be swapped,
##                 and that critical() used $2 instead of $1
## Creator      :  Cheewaphat Loyjew 
## Created Date :  2017-11-27
###########
## Usage :  ./logging.bash <CONTENT> <DATE FORMAT>


exec 3>&2 # logging stream (file descriptor 3) defaults to STDERR
verbosity=5 # default to show warnings
silent_lvl=0
crt_lvl=1
err_lvl=2
wrn_lvl=3
inf_lvl=4
dbg_lvl=5

log_notify() { log $silent_lvl "NOTE: $1"; } # Always prints
log_critical() { log $crt_lvl "CRITICAL: $1"; }
log_error() { log $err_lvl "ERROR: $1"; }
log_warn() { log $wrn_lvl "WARNING: $1"; }
log_inf() { log $inf_lvl "INFO: $1"; } # "info" is already a command
log_debug() { log $dbg_lvl "DEBUG: $1"; }
log() {
    if [ $verbosity -ge $1 ]; then
        datestring=`date +'%Y-%m-%d %H:%M:%S'`
        # Expand escaped characters, wrap at 70 chars, indent wrapped lines
        echo -e "$datestring $2" | fold -w250 -s | sed '2~1s/^/  /' >&3
    fi
}