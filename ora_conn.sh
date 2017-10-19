#!/bin/bash


ORACLE_HOME=/opt/app/oracle/dbhdpmdv1/product/11.2.0.4/dbhome_1/
LD_LIBRARY_PATH=${ORACLE_HOME%/}/bin/

export ORACLE_HOME
export LD_LIBRARY_PATH

echo $ORACLE_HOME
echo $LD_LIBRARY_PATH


#### connect ####
ORA_USERNAME=NRTSTGAPPO
ORA_PASSWORD=nrtstgappo_dev
ORA_SERVER=172.19.195.90
ORA_PORT=1532
ORA_SERVICE_NAME=ODSDEV2


sSQL="SELECT 1 FROM DUAL"
result=`${ORACLE_HOME}/bin/sqlplus -s ${ORA_USERNAME}/${ORA_PASSWORD}@${ORA_SERVER}:${ORA_PORT}/${ORA_SERVICE_NAME} <<EOF
SET PAGESIZE 0 FEEDBACK OFF VERIFY OFF HEADING OFF ECHO OFF
${sSQL};
EXIT;
EOF`

echo $result


### sqlldr
cmd_sqlldr="${ORACLE_HOME}/bin/sqlldr userid=${ORA_USERNAME}/${ORA_PASSWORD}@${ORA_SERVER}:${ORA_PORT}/${ORA_SERVICE_NAME} control=tgw_ccp_dev.ctl  log=/tmp/workspace/kafka-python/sqlldr_kafkaf.log"

echo $cmd_sqlldr
eval $cmd_sqlldr

echo "Done"
