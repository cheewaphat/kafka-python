import cx_Oracle

con = cx_Oracle.connect('nrtstgappo/nrtstgappo_dev@172.19.195.90:1532/nrtstgappo')
print con.version

con.close()


#user/pass : nrtstgappo/nrtstgappo_dev@ODSDEV2.TRUE.TH
ODSDEV2.TRUE.TH=
	(DESCRIPTION=
      (ADDRESS=
        (PROTOCOL=TCP)
        (HOST=172.19.195.90)
        (PORT=1532)
      )
      (CONNECT_DATA=
        (SERVER=dedicated)
        (SERVICE_NAME=ODSDEV2)
      )
    )


    nrtstgappo/nrtstgappo_dev@ODSDEV2.TRUE.TH

    export ORACLE_HOME=/opt/app/oracle/hdpidbpr1/product/11.2.0/client_1 && /opt/app/oracle/hdpidbpr1/product/11.2.0/client_1/bin/sqlplus -s nrtstgappo/nrtstgappo_dev@172.19.195.90:1532/ODSDEV2