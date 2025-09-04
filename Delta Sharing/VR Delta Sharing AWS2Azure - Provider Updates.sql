-- RESTORE
RESTORE TABLE vr_demo.crisp_share.sales VERSION AS OF 0;

-- UPDATE
update vr_demo.crisp_share.sales set sales_amount = 1000 where sales_id = 4025506998187779871;

-- DELETE
delete from vr_demo.crisp_share.sales where sales_id = 1614973159450068302;

-- INSERT
insert into vr_demo.crisp_share.sales 
select 
  date_key,
  0 as sales_id,
  * except(date_key, sales_id, sales_amount),
  2000 as sales_amount
from vr_demo.crisp_share.sales 
where sales_id = 5961426756501796421;