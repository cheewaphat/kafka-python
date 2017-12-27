#!/bin/bash
###########        Info        ###########
## Script Name  :  mail_send.sh
## Description  :  send mail detail process logs
## Creator      :  Cheewaphat Loyjew 
## Created Date :  2017-11-08
###########
## Usage :  ./mail_send.sh <MAIL TO> <SUBJECT> <CONTENT> 

if [ -z "$1" ]
then 
  echo "`date '+[%Y-%m-%d %H:%M:%S]'` Error, $0 No Arguments Supplied"
  exit 1
fi

CURR_DIR="$( cd "$( dirname "${0}" )" && pwd )"
CURR_USER="`whoami`"

# process
if [ -f $3 ]; then
  cat "$3" | mail -s "$2" "$1" || echo "send mail fail"
  rm -f "$3"
else
  echo "$3" | mail -s "$2" "$1" || echo "send mail fail"
fi