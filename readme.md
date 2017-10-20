#sqoop
kinit -k -t cisetl1b.keytab  cisetl1b@SAMBA-TEST.TH

hadoop fs -rm /user/cisetl1b/sqoop_import/SQOOP_TEDW_BSNS_INTRACN_SR/*
hadoop fs -rmdir /user/cisetl1b/sqoop_import/SQOOP_TEDW_BSNS_INTRACN_SR

sqoop import 
-Dhadoop.security.credential.provider.path=jceks://hdfs/user/cisetl1b/sqoop_import/sqoop_config/sqoop_Oracle_TEDW.pwd.jceks --connect jdbc:oracle:thin:@'(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=172.19.196.25)(PORT=1550))(ADDRESS=(PROTOCOL=TCP)(HOST=172.19.196.26)(PORT=1550))(ADDRESS=(PROTOCOL=TCP)(HOST=172.19.196.27)(PORT=1550))(CONNECT_DATA=(SERVER=DEDICATED)(SERVICE_NAME=TEDWDEV)))' --username "TEDWDWHAPPB" --password-alias TEDWdb.password.alias --table "TEDWDWHAPPO.BSNS_INTRACN_SR" --columns "BSNS_INTRACN_KEY,BSNS_INTRACN_ID" --target
-dir "/user/cisetl1b/sqoop_import/SQOOP_TEDW_BSNS_INTRACN_SR" --verbose

result=`${ORACLE_HOME}/bin/sqlplus -s ${ORA_USERNAME}/${ORA_PASSWORD}@${ORA_SERVER}:${ORA_PORT}/${ORA_SERVICE_NAME}


TRAILING NULLCOLS( 
    ODS_PPN_BY constant "TEST_BY_JOE", 
    ODS_ACTION constant "I", 
) 
