insert into tgw_ccp_prod
( notification_id, event_begintime, msisdn, event_type, imsi, subscriber_priceplancode, def_language, chain_id, activation_date, topping_id, effective_date, expire_date, price, account_type, old_priceplancode, new_priceplancode, request_id, balance_type, amount, new_balance, validity, new_validity, subscriber_priceplan_id, message_type, service_id, service_type, transacion_sn, accum_count, accum_amount, cella, ods_ppn_by, ods_ppn_tm, ods_upd_by, ods_upd_tm, ods_action, osb_receivetime)
select 
tmp.notification_id, tmp.event_begintime, tmp.msisdn, tmp.event_type, tmp.imsi, tmp.subscriber_priceplancode, tmp.def_language, tmp.chain_id, tmp.activation_date, tmp.topping_id, tmp.effective_date, tmp.expire_date, tmp.price, tmp.account_type, tmp.old_priceplancode, tmp.new_priceplancode, tmp.request_id, tmp.balance_type, tmp.amount, tmp.new_balance, tmp.validity, tmp.new_validity, tmp.subscriber_priceplan_id, tmp.message_type, tmp.service_id, tmp.service_type, tmp.transacion_sn, tmp.accum_count, tmp.accum_amount, tmp.cella, tmp.ods_ppn_by, tmp.ods_ppn_tm, 'ODSMIGRATION' AS ods_upd_by, sysdate , tmp.ods_action, tmp.osb_receivetime 
from TGW_CCP  tmp 
right join TGW_CCP_PROD prod on ( tmp.notification_id <> prod.notification_id );
where prod.event_begintime < SYSDATE-1
commit;