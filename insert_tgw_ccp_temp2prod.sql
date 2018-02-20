-- using insert into
insert into tgw_ccp_prod
( notification_id, event_begintime, msisdn, event_type, imsi, subscriber_priceplancode, def_language, chain_id, activation_date, topping_id, effective_date, expire_date, price, account_type, old_priceplancode, new_priceplancode, request_id, balance_type, amount, new_balance, validity, new_validity, subscriber_priceplan_id, message_type, service_id, service_type, transacion_sn, accum_count, accum_amount, cella, ods_ppn_by, ods_ppn_tm, ods_upd_by, ods_upd_tm, ods_action, osb_receivetime)
select 
tmp.notification_id, tmp.event_begintime, tmp.msisdn, tmp.event_type, tmp.imsi, tmp.subscriber_priceplancode, tmp.def_language, tmp.chain_id, tmp.activation_date, tmp.topping_id, tmp.effective_date, tmp.expire_date, tmp.price, tmp.account_type, tmp.old_priceplancode, tmp.new_priceplancode, tmp.request_id, tmp.balance_type, tmp.amount, tmp.new_balance, tmp.validity, tmp.new_validity, tmp.subscriber_priceplan_id, tmp.message_type, tmp.service_id, tmp.service_type, tmp.transacion_sn, tmp.accum_count, tmp.accum_amount, tmp.cella, tmp.ods_ppn_by, tmp.ods_ppn_tm, 'ODSMIGRATION' AS ods_upd_by, sysdate , tmp.ods_action, tmp.osb_receivetime 
from TGW_CCP  tmp 
right join TGW_CCP_PROD prod on ( tmp.notification_id <> prod.notification_id );
where prod.event_begintime < SYSDATE-1
commit;


-- using merge
MERGE INTO tgw_ccp_prod a
USING (
SELECT notification_id,event_begintime,msisdn,event_type,imsi,subscriber_priceplancode,def_language,chain_id,activation_date,topping_id,effective_date,expire_date,price,account_type,old_priceplancode,new_priceplancode,request_id,balance_type,amount,new_balance,validity,new_validity,subscriber_priceplan_id,message_type,service_id,service_type,transacion_sn,accum_count,accum_amount,cella,ods_ppn_by,ods_ppn_tm,ods_upd_by,ods_upd_tm,ods_action,osb_receivetime
FROM tgw_ccp
) b ON (a.notification_id = b.notification_id)
WHEN NOT MATCHED THEN
  INSERT ( a.notification_id,a.event_begintime,a.msisdn,a.event_type,a.imsi,a.subscriber_priceplancode,a.def_language,a.chain_id,a.activation_date,a.topping_id,a.effective_date,a.expire_date,a.price,a.account_type,a.old_priceplancode,a.new_priceplancode,a.request_id,a.balance_type,a.amount,a.new_balance,a.validity,a.new_validity,a.subscriber_priceplan_id,a.message_type,a.service_id,a.service_type,a.transacion_sn,a.accum_count,a.accum_amount,a.cella,a.ods_ppn_by,a.ods_ppn_tm,a.ods_upd_by,a.ods_upd_tm,a.ods_action,a.osb_receivetime)
  VALUES ( b.notification_id,b.event_begintime,b.msisdn,b.event_type,b.imsi,b.subscriber_priceplancode,b.def_language,b.chain_id,b.activation_date,b.topping_id,b.effective_date,b.expire_date,b.price,b.account_type,b.old_priceplancode,b.new_priceplancode,b.request_id,b.balance_type,b.amount,b.new_balance,b.validity,b.new_validity,b.subscriber_priceplan_id,b.message_type,b.service_id,b.service_type,b.transacion_sn,b.accum_count,b.accum_amount,b.cella,b.ods_ppn_by,b.ods_ppn_tm,b.ods_upd_by,b.ods_upd_tm,b.ods_action,b.osb_receivetime);
commit;
