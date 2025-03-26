create table ods_order_info (
                                `id` string COMMENT '�������',
                                `total_amount` decimal(10,2) COMMENT '�������',
                                `order_status` string COMMENT '����״̬',
                                `user_id` string COMMENT '�û�id' ,
                                `payment_way` string COMMENT '֧����ʽ',
                                `out_trade_no` string COMMENT '֧����ˮ��',
                                `create_time` string COMMENT '����ʱ��',
                                `operate_time` string COMMENT '����ʱ��'
) COMMENT '������'
PARTITIONED BY ( `dt` string)
row format delimited  fields terminated by '\t'
location '/user/hive/warehouse/dev_realtime_v1_wenzhen_xie.db/ods/ods_order_info/'
tblproperties ("parquet.compression"="snappy");

-- drop table if exists ods_order_detail;
create table ods_order_detail(
                                 `id` string COMMENT '�������',
                                 `order_id` string  COMMENT '������',
                                 `user_id` string COMMENT '�û�id' ,
                                 `sku_id` string COMMENT '��Ʒid',
                                 `sku_name` string COMMENT '��Ʒ����',
                                 `order_price` string COMMENT '�µ��۸�',
                                 `sku_num` string COMMENT '��Ʒ����',
                                 `create_time` string COMMENT '����ʱ��'
) COMMENT '������ϸ��'
PARTITIONED BY ( `dt` string)
row format delimited  fields terminated by '\t'
location '/user/hive/warehouse/dev_realtime_v1_wenzhen_xie.db/ods/ods_order_detail/'
tblproperties ("parquet.compression"="snappy");


-- drop table if exists ods_sku_info;
create table ods_sku_info(
                             `id` string COMMENT 'skuId',
                             `spu_id` string  COMMENT 'spuid',
                             `price` decimal(10,2) COMMENT '�۸�' ,
                             `sku_name` string COMMENT '��Ʒ����',
                             `sku_desc` string COMMENT '��Ʒ����',
                             `weight` string COMMENT '����',
                             `tm_id` string COMMENT 'Ʒ��id',
                             `category3_id` string COMMENT 'Ʒ��id',
                             `create_time` string COMMENT '����ʱ��'
) COMMENT '��Ʒ��'
PARTITIONED BY ( `dt` string)
row format delimited  fields terminated by '\t'
location '/user/hive/warehouse/dev_realtime_v1_wenzhen_xie.db/ods/ods_sku_info/'
tblproperties ("parquet.compression"="snappy");


-- drop table if exists ods_user_info;
create table ods_user_info(
                              `id` string COMMENT '�û�id',
                              `name`  string COMMENT '����',
                              `birthday` string COMMENT '����' ,
                              `gender` string COMMENT '�Ա�',
                              `email` string COMMENT '����',
                              `user_level` string COMMENT '�û��ȼ�',
                              `create_time` string COMMENT '����ʱ��'
) COMMENT '�û���Ϣ'
PARTITIONED BY ( `dt` string)
row format delimited  fields terminated by '\t'
location '/user/hive/warehouse/dev_realtime_v1_wenzhen_xie.db/ods/ods_user_info/'
tblproperties ("parquet.compression"="snappy");


-- drop table if exists ods_base_category1;
create table ods_base_category1(
                                   `id` string COMMENT 'id',
                                   `name`  string COMMENT '����'
) COMMENT '��Ʒһ������'
PARTITIONED BY ( `dt` string)
row format delimited  fields terminated by '\t'
location '/user/hive/warehouse/dev_realtime_v1_wenzhen_xie.db/ods/ods_base_category1/'
tblproperties ("parquet.compression"="snappy");



-- drop table if exists ods_base_category2;
create external table ods_base_category2(
  `id` string COMMENT ' id',
 `name`  string COMMENT '����',
  category1_id string COMMENT 'һ��Ʒ��id'
) COMMENT '��Ʒ��������'
PARTITIONED BY ( `dt` string)
row format delimited  fields terminated by '\t'
location '/user/hive/warehouse/dev_realtime_v1_wenzhen_xie.db/ods/ods_base_category2/'
tblproperties ("parquet.compression"="snappy");



-- drop table if exists ods_base_category3;
create table ods_base_category3(
                                   `id` string COMMENT ' id',
                                   `name`  string COMMENT '����',
                                   category2_id string COMMENT '����Ʒ��id'
) COMMENT '��Ʒ��������'
PARTITIONED BY ( `dt` string)
row format delimited  fields terminated by '\t'
location '/user/hive/warehouse/dev_realtime_v1_wenzhen_xie.db/ods/ods_base_category3/'
tblproperties ("parquet.compression"="snappy");



-- drop table if exists `ods_payment_info`;
create table  `ods_payment_info`(
                                    `id`  bigint COMMENT '���',
                                    `out_trade_no`  string COMMENT '����ҵ����',
                                    `order_id`  string COMMENT '�������',
                                    `user_id` string COMMENT '�û����',
                                    `alipay_trade_no` string COMMENT '֧����������ˮ���',
                                    `total_amount`  decimal(16,2) COMMENT '֧�����',
                                    `subject`  string COMMENT '��������',
                                    `payment_type` string COMMENT '֧������',
                                    `payment_time`  string COMMENT '֧��ʱ��'
)  COMMENT '֧����ˮ��'
PARTITIONED BY ( `dt` string)
row format delimited  fields terminated by '\t'
location '/user/hive/warehouse/dev_realtime_v1_wenzhen_xie.db/ods/ods_payment_info/'
tblproperties ("parquet.compression"="snappy");

CREATE TABLE ods_comment_info (
                                  id BIGINT COMMENT '���',
                                  user_id BIGINT COMMENT '�û�����',
                                  sku_id BIGINT COMMENT 'skuid',
                                  spu_id BIGINT COMMENT '��Ʒid',
                                  order_id BIGINT COMMENT '�������',
                                  appraise STRING COMMENT '���� 1 ���� 2 ���� 3 ����',
                                  comment_txt STRING COMMENT '��������',
                                  create_time TIMESTAMP COMMENT '����ʱ��',
                                  operate_time TIMESTAMP COMMENT '�޸�ʱ��'
)
    COMMENT '��Ʒ���۱�'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/user/hive/warehouse/dev_realtime_v1_wenzhen_xie.db/ods/ods_comment_info/'
TBLPROPERTIES (
    'hive.exec.compress.output'='true',
    'mapreduce.output.fileoutputformat.compress'='true',
    'mapreduce.output.fileoutputformat.compress.codec'='org.apache.hadoop.io.compress.GzipCodec'
);

load data inpath '/2207A/wenzhen_xie/order_info/2025-03-24' OVERWRITE into table ods_order_info partition(dt='2025-03-24');
load data inpath '/2207A/wenzhen_xie/order_detail/2025-03-24' OVERWRITE into table  ods_order_detail partition(dt='2025-03-24');
load data inpath '/2207A/wenzhen_xie/sku_info/2025-03-24' OVERWRITE into table  ods_sku_info partition(dt='2025-03-24');
load data inpath '/2207A/wenzhen_xie/user_info/2025-03-24' OVERWRITE into table  ods_user_info partition(dt='2025-03-24');
load data inpath '/2207A/wenzhen_xie/payment_info/2025-03-24' OVERWRITE into table  ods_payment_info partition(dt='2025-03-24');
load data inpath '/2207A/wenzhen_xie/base_category1/2025-03-24' OVERWRITE into table  ods_base_category1 partition(dt='2025-03-24');
load data inpath '/2207A/wenzhen_xie/base_category2/2025-03-24' OVERWRITE into table  ods_base_category2 partition(dt='2025-03-24');
load data inpath '/2207A/wenzhen_xie/base_category3/2025-03-24' OVERWRITE into table  ods_base_category3 partition(dt='2025-03-24');
load data inpath '/2207A/wenzhen_xie/comment_info/2025-03-24' OVERWRITE into table  ods_comment_info partition(dt='2025-03-24');



create external table dwd_order_info (
`id` string COMMENT '',
`total_amount` decimal(10,2) COMMENT '',
`order_status` string COMMENT ' 1 2  3  4  5',
`user_id` string COMMENT 'id' ,
`payment_way` string COMMENT '',
`out_trade_no` string COMMENT '',
`create_time` string COMMENT '',
`operate_time` string COMMENT ''
) COMMENT ''
PARTITIONED BY ( `dt` string)
stored as  parquet
location '/user/hive/warehouse/dev_realtime_v1_wenzhen_xie.db/dwd/dwd_order_info/'
tblproperties ("parquet.compression"="snappy");



create external table dwd_order_detail(
`id` string COMMENT '',
`order_id` decimal(10,2) COMMENT '',
`user_id` string COMMENT 'id' ,
`sku_id` string COMMENT 'id',
`sku_name` string COMMENT '',
`order_price` string COMMENT '',
`sku_num` string COMMENT '',
`create_time` string COMMENT ''
) COMMENT ''
PARTITIONED BY ( `dt` string)
stored as  parquet
location '/user/hive/warehouse/dev_realtime_v1_wenzhen_xie.db/dwd/dwd_order_detail/'
tblproperties ("parquet.compression"="snappy");

create external table dwd_user_info(
 `id` string COMMENT 'id',
 `name`  string COMMENT '',
 `birthday` string COMMENT '' ,
 `gender` string COMMENT '',
 `email` string COMMENT '',
 `user_level` string COMMENT '',
 `create_time` string COMMENT ''
) COMMENT ''
PARTITIONED BY ( `dt` string)
stored as  parquet
location '/user/hive/warehouse/dev_realtime_v1_wenzhen_xie.db/dwd/dwd_user_info/'
tblproperties ("parquet.compression"="snappy");



create external  table  `dwd_payment_info`(
`id` bigint COMMENT '',
`out_trade_no` string COMMENT '',
`order_id`  string COMMENT '',
`user_id`  string COMMENT '',
`alipay_trade_no` string COMMENT '',
`total_amount`  decimal(16,2) COMMENT '',
`subject`  string COMMENT '',
`payment_type` string COMMENT '',
`payment_time`  string COMMENT ''
 )  COMMENT ''
PARTITIONED BY ( `dt` string)
stored as  parquet
location '/user/hive/warehouse/dev_realtime_v1_wenzhen_xie.db/dwd/dwd_payment_info/'
tblproperties ("parquet.compression"="snappy");

create external table dwd_sku_info(
  `id` string COMMENT 'skuId',
  `spu_id` string COMMENT 'spuid',
 `price` decimal(10,2) COMMENT '' ,
`sku_name` string COMMENT '',
 `sku_desc` string COMMENT '',
 `weight` string COMMENT '',
 `tm_id` string COMMENT 'id',
 `category3_id` string COMMENT '1id',
 `category2_id` string COMMENT '2id',
 `category1_id` string COMMENT '3id',
 `category3_name` string COMMENT '3',
 `category2_name` string COMMENT '2',
 `category1_name` string COMMENT '1',
 `create_time` string COMMENT ''
) COMMENT ''
PARTITIONED BY ( `dt` string)
stored as  parquet
location '/user/hive/warehouse/dev_realtime_v1_wenzhen_xie.db/dwd/dwd_sku_info/'
tblproperties ("parquet.compression"="snappy");

CREATE TABLE if not exists  dwd_comment_info (
                                                 id bigint COMMENT '���',
                                                 user_id STRING COMMENT '�û�����',
                                                 sku_id STRING COMMENT 'skuid',
                                                 spu_id STRING COMMENT '��Ʒid',
                                                 order_id bigint COMMENT '�������',
                                                 appraise string COMMENT '���� 1 ���� 2 ���� 3 ����',
                                                 comment_txt string COMMENT '��������',
                                                 create_time string COMMENT '����ʱ��',
                                                 operate_time string COMMENT '�޸�ʱ��'
) COMMENT '��Ʒ���۱�'
    PARTITIONED BY ( `dt` string)
    row format delimited  fields terminated by '\t'
    location '/user/hive/warehouse/dev_realtime_v1_wenzhen_xie.db/dwd/dwd_comment_info'
    tblproperties ("parquet.compression"="snappy");

set hive.exec.dynamic.partition.mode=nonstrict;

insert  overwrite table  dwd_comment_info partition(dt)
select  * from ods_comment_info
where dt='2025-03-24'   and id is not null;

insert  overwrite table   dwd_order_info partition(dt)
select  * from ods_order_info
where dt='2025-03-24'  and id is not null;

insert  overwrite table   dwd_order_detail partition(dt)
select  * from ods_order_detail
where dt='2025-03-24'  and id is not null;

insert  overwrite table   dev_realtime_v1_wenzhen_xie.dwd_user_info partition(dt)
select  * from dev_realtime_v1_wenzhen_xie.ods_user_info
where dt='2025-03-24'   and id is not null;


insert  overwrite table   dev_realtime_v1_wenzhen_xie.dwd_payment_info partition(dt)
select  * from dev_realtime_v1_wenzhen_xie.ods_payment_info
where dt='2025-03-24'  and id is not null;


insert  overwrite table   dev_realtime_v1_wenzhen_xie.dwd_sku_info partition(dt)
select
    sku.id,
    sku.spu_id,
    sku.price,
    sku.sku_name,
    sku.sku_desc,
    sku.weight,
    sku.tm_id,
    sku.category3_id,
    c2.id category2_id ,
    c1.id category1_id,
    c3.name category3_name,
    c2.name category2_name,
    c1.name category1_name,
    sku.create_time,
    sku.dt
from
    dev_realtime_v1_wenzhen_xie.ods_sku_info sku
        join dev_realtime_v1_wenzhen_xie.ods_base_category3 c3 on sku.category3_id=c3.id
        join dev_realtime_v1_wenzhen_xie.ods_base_category2 c2 on c3.category2_id=c2.id
        join dev_realtime_v1_wenzhen_xie.ods_base_category1 c1 on c2.category1_id=c1.id
where sku.dt='2025-03-24'  and c2.dt='2025-03-24'
  and  c3.dt='2025-03-24' and  c1.dt='2025-03-24'
  and sku.id is not null;

create  external table dws_user_action
(
    user_id         string      comment '�û� id',
    order_count     bigint      comment '�µ����� ',
    order_amount    decimal(16,2)  comment '�µ���� ',
    payment_count   bigint      comment '֧������',
    payment_amount  decimal(16,2) comment '֧����� ',
    comment_count   bigint      comment '���۴���'
) COMMENT 'ÿ���û���Ϊ���'
PARTITIONED BY ( `dt` string)
stored as  parquet
location '/user/hive/warehouse/dev_realtime_v1_wenzhen_xie.db/dws/dws_user_action/'
tblproperties ("parquet.compression"="snappy");
with
    tmp_order as
        (
            select
                user_id,
                sum(oc.total_amount) order_amount,
                count(*)  order_count
            from dwd_order_info  oc
            where date_format(oc.create_time,'yyyy-MM-dd')='2025-03-24'
            group by user_id
        )  ,
    tmp_payment as
        (
            select
                user_id,
                sum(pi.total_amount) payment_amount,
                count(*) payment_count
            from dwd_payment_info pi
            where date_format(pi.payment_time,'yyyy-MM-dd')='2025-03-12'
            group by user_id
        ),
    tmp_comment as
        (
            select
                user_id,
                count(*) comment_count
            from dwd_comment_info c
            where date_format(c.dt,'yyyy-MM-dd')='2025-03-24'
            group by user_id
        )

insert overwrite table dws_user_action partition(dt='2025-03-24')
select
    user_actions.user_id,
    sum(user_actions.order_count),
    sum(user_actions.order_amount),
    sum(user_actions.payment_count),
    sum(user_actions.payment_amount),
    sum(user_actions.comment_count)
from
    (
        select
            user_id,
            order_count,
            order_amount ,
            0 payment_count ,
            0 payment_amount,
            0 comment_count
        from tmp_order

        union all
        select
            user_id,
            0,
            0,
            payment_count,
            payment_amount,
            0
        from tmp_payment

        union all
        select
            user_id,
            0,
            0,
            0,
            0,
            comment_count
        from tmp_comment
    ) user_actions
group by user_id;


create table ads_gmv_sum_day(
                                `dt` string COMMENT 'ͳ������',
                                `gmv_count`  bigint COMMENT '����gmv��������',
                                `gmv_amount`  decimal(16,2) COMMENT '����gmv�����ܽ��',
                                `gmv_payment`  decimal(16,2) COMMENT '����֧�����'
) COMMENT 'ÿ�ջ�Ծ�û�����'
row format delimited  fields terminated by '\t'
location '/user/hive/warehouse/dev_realtime_v1_wenzhen_xie.db/ads/ads_gmv_sum_day/';


insert into table ads_gmv_sum_day
select
    '2025-03-24' dt ,
    sum(order_count)  gmv_count ,
    sum(order_amount) gmv_amount ,
    sum(payment_amount) payment_amount
from dws_user_action
where dt ='2025-03-24'
group by dt;





create external table  dws_sale_detail_daycount
( user_id  string  comment '�û� id',
 sku_id  string comment '��Ʒ Id',
 user_gender  string comment '�û��Ա�',
 user_age string  comment '�û�����',
 user_level string comment '�û��ȼ�',
 order_price decimal(10,2) comment '�����۸�',
 sku_name string  comment '��Ʒ����',
 sku_tm_id string  comment 'Ʒ��id',
 sku_category3_id string comment '��Ʒ����Ʒ��id',
 sku_category2_id string comment '��Ʒ����Ʒ��id',
 sku_category1_id string comment '��Ʒһ��Ʒ��id',
sku_category3_name string comment '��Ʒ����Ʒ������',
 sku_category2_name string comment '��Ʒ����Ʒ������',
sku_category1_name string comment '��Ʒһ��Ʒ������',
 spu_id  string comment '��Ʒ spu',
 sku_num  int comment '�������',
 order_count string comment '�����µ�����',
 order_amount string comment '�����µ����'
) COMMENT '�û�������Ʒ��ϸ��'
PARTITIONED BY ( `dt` string)
stored as  parquet
location '/user/hive/warehouse/dev_realtime_v1_wenzhen_xie.db/dws/dws_user_sale_detail_daycount/'
tblproperties ("parquet.compression"="snappy");
with
    tmp_detail as
        (
            select
                user_id,
                sku_id,
                sum(sku_num) sku_num ,
                count(*) order_count ,
                sum(od.order_price*sku_num)  order_amount
            from ods_order_detail od
            where od.dt='2025-03-24' and user_id is not null
            group by user_id, sku_id
        )
insert overwrite table  dws_sale_detail_daycount partition(dt='2025-03-24')
select
    tmp_detail.user_id,
    tmp_detail.sku_id,
    u.gender,
    months_between('2025-03-12', u.birthday)/12  age,
    u.user_level,
    price,
    sku_name,
    tm_id,
    category3_id ,
    category2_id ,
    category1_id ,
    category3_name ,
    category2_name ,
    category1_name ,
    spu_id,
    tmp_detail.sku_num,
    tmp_detail.order_count,
    tmp_detail.order_amount
from tmp_detail
         left join dwd_user_info u on u.id=tmp_detail.user_id  and u.dt='2025-03-24'
         left join dwd_sku_info s on tmp_detail.sku_id =s.id  and s.dt='2025-03-24';

create  table ads_sale_tm_category1_stat_mn
(
    tm_id string comment 'Ʒ��id ' ,
    category1_id string comment '1��Ʒ��id ',
    category1_name string comment '1��Ʒ������ ',
    buycount   bigint comment  '��������',
    buy_twice_last bigint  comment '�������Ϲ�������',
    buy_twice_last_ratio decimal(10,2)  comment  '���θ�����',
    buy_3times_last   bigint comment   '�������Ϲ�������',
    buy_3times_last_ratio decimal(10,2)  comment  '��θ�����' ,
    stat_mn string comment 'ͳ���·�',
    stat_date string comment 'ͳ������'
)   COMMENT '������ͳ��'
row format delimited  fields terminated by '\t'
location '/user/hive/warehouse/dev_realtime_v1_wenzhen_xie.db/ads/ads_sale_tm_category1_stat_mn/';


insert into table ads_sale_tm_category1_stat_mn
select
    mn.sku_tm_id,
    mn.sku_category1_id,
    mn.sku_category1_name,
    sum(if(mn.order_count>=1,1,0)) buycount,
    sum(if(mn.order_count>=2,1,0)) buyTwiceLast,
    sum(if(mn.order_count>=2,1,0))/sum( if(mn.order_count>=1,1,0)) buyTwiceLastRatio,
    sum(if(mn.order_count>3,1,0))  buy3timeLast  ,
    sum(if(mn.order_count>=3,1,0))/sum( if(mn.order_count>=1,1,0)) buy3timeLastRatio ,
    date_format('2025-03-24' ,'yyyy-MM') stat_mn,
    '2025-03-24' stat_date
from
    (
        select od.sku_tm_id,
               od.sku_category1_id,
               od.sku_category1_name,
               user_id ,
               sum(order_count) order_count
        from  dws_sale_detail_daycount  od
        where
                date_format(dt,'yyyy-MM')<=date_format('2025-03-24' ,'yyyy-MM')
        group by
            od.sku_tm_id, od.sku_category1_id, user_id, od.sku_category1_name
    ) mn
group by mn.sku_tm_id, mn.sku_category1_id, mn.sku_category1_name;







