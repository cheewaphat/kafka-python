select column_name , data_type, decode(column_id ,(SELECT max(column_id) FROM cols WHERE table_name like 'TGW_CCP'  )
                                                     ,decode(DATA_TYPE,'DATE','TIMESTAMP','STRING') 
                                                     ,decode(DATA_TYPE,'DATE','TIMESTAMP','STRING') 
                                                     )
from cols 
where table_name like 'TGW_CCP' 
order by column_id asc