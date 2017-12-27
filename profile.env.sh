##**********************************************************************************************************##
## Script Name  : profile.env                                                                              ##
## Description  : TGW Script Environment Configuration (Set environment variables here).                     ##
## Creator      : Cheewaphat Loy/True IT.                                                                                ##
## Created Date : 2017-09-11                                                                                ##
##**********************************************************************************************************##
## set -x                                    # For debug only.

###########***********************************###########
########### Hadoop Setup&Config               ###########
###########***********************************###########
export HADOOP_KERBEROS_USER=cisetl1b
export HADOOP_KERBEROS_HOST=hdpwdbdv1
export HADOOP_KERBEROS_REALM=SAMBA-TEST.TH
export HADOOP_IMPALA_DB_NAME=UAT_TSID

###########***********************************###########
########### AUX Setup&Config                  ###########
###########***********************************###########
export AUX_CONNECT_STRING=172.19.196.25:1550/TEDWDEV
export AUX_USERNAME=DVAUXAPPO
export AUX_PASSWORD_CRYPT="cEBzc3cwcmQ="
export AUX_PASSWORD=`echo -n "${AUX_PASSWORD_CRYPT}" | base64 -d`
export AUX_DATABASE=DVAUXAPPO
export AUX_ENV='HDP_TLS'
## Remark ## How to encode :  echo -n "p@ssw0rd" | base64

###########***********************************###########
########### Oracle Environment&Variables      ###########
###########***********************************###########
export ORACLE_HOME=`find /opt/app/oracle/ -name sqlplus -print 2> /dev/null | egrep "bin" | head -1 | sed "s/bin\/sqlplus//g"`
echo $ORACLE_HOME
export PATH=$PATH:$ORACLE_HOME/bin
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$ORACLE_HOME/lib

###########***********************************###########
###########        Server Information         ###########
###########***********************************###########
hostname
id

###########***********************************###########
###########        Kerberos Initial           ###########
###########***********************************###########
# kinit -k -t cisetl1b.keytab  cisetl1b@SAMBA-TEST.TH
echo "kinit -k -t profile.keytab  ${HADOOP_KERBEROS_USER}@${HADOOP_KERBEROS_REALM}"
kinit -k -t profile.keytab  ${HADOOP_KERBEROS_USER}@${HADOOP_KERBEROS_REALM}
klist

###########***********************************###########
###########        Function  Declatation      ###########
###########***********************************###########

### Execute AUX Command Line. ###
function ExecAuxCmd()         
{
        i_stepName=$1          # Get value of Step Name       e.q. 'SQOOP_TEDW_COMMON__BSNS_INTRACN_SR'
        i_cmd=$2               # Get value of Command Line    e.q. 'Select * from HDPAUX_DELTA_LOAD; '
        v_cmd_result_file="/tmp/${i_stepName}_sql_cmd_result.log"

        sqlplus  -s ${AUX_USERNAME}/\"${AUX_PASSWORD}\"@${AUX_CONNECT_STRING}  <<< "${i_cmd} "  > ${v_cmd_result_file}

        cat ${v_cmd_result_file}
        rm -f ${v_cmd_result_file}
  
}

### Check Error ###
function CheckError()         
{
        i_result=$1            # Get result of Execute Command Line.
        i_cmd=$2               # Get value of Command Line    e.q. 'Select * from HDPAUX_DELTA_LOAD; '
        i_type=${3^^}          #  e.q. 'AUX'
        i_auxTableName=$4      # Get value of AUX Table Name  e.q. 'HDPAUX_DELTA_LOAD','HDPAUX_P_CONTROL','HDP_PROCESS_LOG'
        i_shortMessage=$5      # Get value of AUX Table Name  e.q. 'HDPAUX_DELTA_LOAD','HDPAUX_P_CONTROL','HDP_PROCESS_LOG'
        v_error_code="ora-|SP2|INVALID|ERROR"

        v_count_error=`echo ${i_result} | egrep "${v_error_code}" | wc -l | bc `

        if [ "${i_type}"  =  "AUX" ]; then
              if [[ ${v_count_error} -ne 0 ]]; then
                    echo "Logging of HDP100TB operation to '${AUX_DATABASE}.${i_auxTableName} ' is failed on `date +'%Y-%m-%d %T'` [ ${i_shortMessage} ]. " 
              else
                    echo "Logging of HDP100TB operation to '${AUX_DATABASE}.${i_auxTableName} ' is Success on `date +'%Y-%m-%d %T'` [ ${i_shortMessage} ]. "
              fi
        fi   # End of AUX Type

        if [[ ${v_count_error} -ne 0 ]]; then
          echo "Command Line : ${i_cmd}"
          echo "Command Line Result : ${i_result}"
          exit 99
        fi
}

### Get Step Name. ###
function GetStepName()         
{
                ## Parameters ##
                # $1 = Variable Name   e.q. 'v_stepName'
                # $2 = Original Step Name e.q. 'MAIN' , 'NotUse'
                # $3 = Prefix Name        e.q. 'TEDW'
                # $4 = Table Name         e.q. 'BSNS_INTRACN_SR'
                if [ "${2}" != "MAIN" ]; then
                  eval "$1='SQOOP_${3}_COMMON__${4}' ";
                fi
}

### Get Insert Name. ###
function GetInsertName()         
{
                ## Parameters ##
                # $1 = Variable Name   e.q. 'p_processId_i','v_stepName_i'
                # $2 = Run Mode           e.q. 'N','R'
                # $3 = Original Name   e.q. 'TEDW_TLS_DAILY_MAIN_BATCH','SQOOP_TEDW_COMMON__BSNS_INTRACN_SR'

                if [ "${2}"  =  "N" ]; then
                     eval "$1='${3}' ";
                elif [ "${2}"  =  "R" ]; then
                     eval "$1='${3}_ReRun' ";
                fi
}
