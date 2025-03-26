use tms01;
-- ��ʵ��صı�
-- order_info
-- 1. �˵���������
drop table if exists ods_order_info_inc;
create external table ods_order_info_inc(
  `op` string comment '��������',
  `after` struct<`id`:bigint,`order_no`:string,`status`:string,`collect_type`:string,`user_id`:bigint,`receiver_complex_id`:bigint,`receiver_province_id`:string,`receiver_city_id`:string,`receiver_district_id`:string,`receiver_address`:string,`receiver_name`:string,`sender_complex_id`:bigint,`sender_province_id`:string,`sender_city_id`:string,`sender_district_id`:string,`sender_name`:string,`payment_type`:string,`cargo_num`:bigint,`amount`:decimal(16,2),`estimate_arrive_time`:string,`distance`:decimal(16,2),`create_time`:string,`update_time`:string,`is_deleted`:string> comment '�޸Ļ����������',
  `before` struct<`id`:bigint,`order_no`:string,`status`:string,`collect_type`:string,`user_id`:bigint,`receiver_complex_id`:bigint,`receiver_province_id`:string,`receiver_city_id`:string,`receiver_district_id`:string,`receiver_address`:string,`receiver_name`:string,`sender_complex_id`:bigint,`sender_province_id`:string,`sender_city_id`:string,`sender_district_id`:string,`sender_name`:string,`payment_type`:string,`cargo_num`:bigint,`amount`:decimal(16,2),`estimate_arrive_time`:string,`distance`:decimal(16,2),`create_time`:string,`update_time`:string,`is_deleted`:string> comment '�޸�ǰ������',
  `ts` bigint comment 'ʱ���'
) comment '�˵���'
	partitioned by (`dt` string comment 'ͳ������')
	ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe'
	location '/warehouse/tms/ods/ods_order_info_inc';

-- order_cargo
-- 2. �˵���ϸ��
drop table if exists ods_order_cargo_inc;
create external table ods_order_cargo_inc(
	`op` string comment '��������',
	`after` struct<`id`:bigint,`order_id`:string,`cargo_type`:string,`volumn_length`:bigint,`volumn_width`:bigint,`volumn_height`:bigint,`weight`:decimal(16,2),`create_time`:string,`update_time`:string,`is_deleted`:string> comment '������޸ĺ������',
	`before` struct<`id`:bigint,`order_id`:string,`cargo_type`:string,`volumn_length`:bigint,`volumn_width`:bigint,`volumn_height`:bigint,`weight`:decimal(16,2),`create_time`:string,`update_time`:string,`is_deleted`:string> comment '�޸�ǰ������',
	`ts` bigint comment 'ʱ���'
) comment '�˵���ϸ��'
	partitioned by (`dt` string comment 'ͳ������')
	ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe'
	location '/warehouse/tms/ods/ods_order_cargo_inc';

-- transport_task
-- 3. ���������
drop table if exists ods_transport_task_inc;
create external table ods_transport_task_inc(
	`op` string comment '��������',
	`after` struct<`id`:bigint,`shift_id`:bigint,`line_id`:bigint,`start_org_id`:bigint,`start_org_name`:string,`end_org_id`:bigint,`end_org_name`:string,`status`:string,`order_num`:bigint,`driver1_emp_id`:bigint,`driver1_name`:string,`driver2_emp_id`:bigint,`driver2_name`:string,`truck_id`:bigint,`truck_no`:string,`actual_start_time`:string,`actual_end_time`:string,`actual_distance`:decimal(16,2),`create_time`:string,`update_time`:string,`is_deleted`:string> comment '������޸ĺ������',
	`before` struct<`id`:bigint,`shift_id`:bigint,`line_id`:bigint,`start_org_id`:bigint,`start_org_name`:string,`end_org_id`:bigint,`end_org_name`:string,`status`:string,`order_num`:bigint,`driver1_emp_id`:bigint,`driver1_name`:string,`driver2_emp_id`:bigint,`driver2_name`:string,`truck_id`:bigint,`truck_no`:string,`actual_start_time`:string,`actual_end_time`:string,`actual_distance`:decimal(16,2),`create_time`:string,`update_time`:string,`is_deleted`:string> comment '�޸�ǰ������',
	`ts` bigint comment 'ʱ���'
) comment '���������'
	partitioned by (`dt` string comment 'ͳ������')
	ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe'
	location '/warehouse/tms/ods/ods_transport_task_inc';

-- order_org_bound
-- 4. �˵�������ת��
drop table if exists ods_order_org_bound_inc;
create external table ods_order_org_bound_inc(
	`op` string comment '��������',
	`after` struct<`id`:bigint,`order_id`:bigint,`org_id`:bigint,`status`:string,`inbound_time`:string,`inbound_emp_id`:bigint,`sort_time`:string,`sorter_emp_id`:bigint,`outbound_time`:string,`outbound_emp_id`:bigint,`create_time`:string,`update_time`:string,`is_deleted`:string> comment '������޸ĺ������',
	`before` struct<`id`:bigint,`order_id`:bigint,`org_id`:bigint,`status`:string,`inbound_time`:string,`inbound_emp_id`:bigint,`sort_time`:string,`sorter_emp_id`:bigint,`outbound_time`:string,`outbound_emp_id`:bigint,`create_time`:string,`update_time`:string,`is_deleted`:string> comment '�޸�֮ǰ������',
	`ts` bigint comment 'ʱ���'
) comment '�˵�������ת��'
	partitioned by (`dt` string comment 'ͳ������')
	ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe'
	location '/warehouse/tms/ods/ods_order_org_bound_inc';

-- ά����صı�
-- user_info
-- 5. �û���Ϣ��
drop table if exists ods_user_info_inc;
create external table ods_user_info_inc(
	`op` string comment '��������',
	`after` struct<`id`:bigint,`login_name`:string,`nick_name`:string,`passwd`:string,`real_name`:string,`phone_num`:string,`email`:string,`user_level`:string,`birthday`:string,`gender`:string,`create_time`:string,`update_time`:string,`is_deleted`:string> comment '������޸ĺ������',
	`before` struct<`id`:bigint,`login_name`:string,`nick_name`:string,`passwd`:string,`real_name`:string,`phone_num`:string,`email`:string,`user_level`:string,`birthday`:string,`gender`:string,`create_time`:string,`update_time`:string,`is_deleted`:string> comment '�޸�֮ǰ������',
	`ts` bigint comment 'ʱ���'
) comment '�û���Ϣ��'
	partitioned by (`dt` string comment 'ͳ������')
	ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe'
	location '/warehouse/tms/ods/ods_user_info_inc';

-- user_address
-- 6. �û���ַ��
drop table if exists ods_user_address_inc;
create external table ods_user_address_inc(
	`op` string comment '��������',
	`after` struct<`id`:bigint,`user_id`:bigint,`phone`:string,`province_id`:bigint,`city_id`:bigint,`district_id`:bigint,`complex_id`:bigint,`address`:string,`is_default`:string,`create_time`:string,`update_time`:string,`is_deleted`:string> comment '������޸ĺ������',
	`before` struct<`id`:bigint,`user_id`:bigint,`phone`:string,`province_id`:bigint,`city_id`:bigint,`district_id`:bigint,`complex_id`:bigint,`address`:string,`is_default`:string,`create_time`:string,`update_time`:string,`is_deleted`:string> comment '�޸�֮ǰ������',
	`ts` bigint comment 'ʱ���'
) comment '�û���ַ��'
	partitioned by (`dt` string comment 'ͳ������')
	ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe'
	location '/warehouse/tms/ods/ods_user_address_inc';

-- base_complex
-- 7. С����
drop table if exists ods_base_complex_full;
create external table ods_base_complex_full(
	`id` bigint comment 'С��ID',
	`complex_name` string comment 'С������',
	`province_id` bigint comment 'ʡ��ID',
	`city_id` bigint comment '����ID',
	`district_id` bigint comment '�����أ�ID',
	`district_name` string comment '�����أ�����',
	`create_time` string comment '����ʱ��',
	`update_time` string comment '����ʱ��',
	`is_deleted` string comment '�Ƿ�ɾ��'
) comment 'С����'
	partitioned by (`dt` string comment 'ͳ������')
	row format delimited fields terminated by '\t'
	null defined as ''
	location '/warehouse/tms/ods/ods_base_complex_full';

-- base_dic
-- 8. �ֵ��
drop table if exists ods_base_dic_full;
create external table ods_base_dic_full(
    `id` bigint comment '��ţ�������',
    `parent_id` bigint comment '�������',
    `name` string comment '����',
    `dict_code` string comment '����',
    `create_time` string comment '����ʱ��',
    `update_time` string comment '����ʱ��',
    `is_deleted` string comment '�Ƿ�ɾ��'
) COMMENT '�����ֵ��'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    NULL DEFINED AS ''
    location '/warehouse/tms/ods/ods_base_dic_full/';

-- base_region_info
-- 9. ������
drop table if exists ods_base_region_info_full;
create external table ods_base_region_info_full(
	`id` bigint COMMENT '����ID',
	`parent_id` bigint COMMENT '��������ID',
	`name` string COMMENT '��������',
	`dict_code` string COMMENT '���루��������',
	`short_name` string COMMENT '���',
	`create_time` string COMMENT '����ʱ��',
	`update_time` string COMMENT '����ʱ��',
	`is_deleted` tinyint COMMENT 'ɾ����ǣ�0:������ 1:���ã�'
) comment '������'
	partitioned by (`dt` string comment 'ͳ������')
	row format delimited fields terminated by '\t'
	null defined as ''
	location '/warehouse/tms/ods/ods_base_region_info_full';

-- base_organ
-- 10. ������
drop table if exists ods_base_organ_full;
create external table ods_base_organ_full(
	`id` bigint COMMENT '����ID',
	`org_name` string COMMENT '��������',
	`org_level` bigint COMMENT '�����ȼ���1Ϊת�����ģ�2Ϊת��վ��',
	`region_id` bigint COMMENT '����ID��1������Ϊcity ,2������Ϊdistrict',
	`org_parent_id` bigint COMMENT '��������ID',
	`points` string COMMENT '����ξ�γ�����꼯��',
	`create_time` string COMMENT '����ʱ��',
	`update_time` string COMMENT '����ʱ��',
	`is_deleted` string COMMENT 'ɾ����ǣ�0:������ 1:���ã�'
) comment '������'
	partitioned by (`dt` string comment 'ͳ������')
	row format delimited fields terminated by '\t'
	null defined as ''
	location '/warehouse/tms/ods/ods_base_organ_full';

-- express_courier
-- 11. ���Ա��Ϣ��
drop table if exists ods_express_courier_full;
create external table ods_express_courier_full(
	`id` bigint COMMENT '���ԱID',
	`emp_id` bigint COMMENT 'Ա��ID',
	`org_id` bigint COMMENT '��������ID',
	`working_phone` string COMMENT '�����绰',
	`express_type` string COMMENT '���Ա���ͣ��ջ���������',
	`create_time` string COMMENT '����ʱ��',
	`update_time` string COMMENT '����ʱ��',
	`is_deleted` string COMMENT 'ɾ����ǣ�0:������ 1:���ã�'
) comment '���Ա��Ϣ��'
	partitioned by (`dt` string comment 'ͳ������')
	row format delimited fields terminated by '\t'
	null defined as ''
	location '/warehouse/tms/ods/ods_express_courier_full';

-- express_courier_complex
-- 12. ���ԱС��������
drop table if exists ods_express_courier_complex_full;
create external table ods_express_courier_complex_full(
	`id` bigint COMMENT '����ID',
	`courier_emp_id` bigint COMMENT '���ԱID',
	`complex_id` bigint COMMENT 'С��ID',
	`create_time` string COMMENT '����ʱ��',
	`update_time` string COMMENT '����ʱ��',
	`is_deleted` string COMMENT 'ɾ����ǣ�0:������ 1:���ã�'
) comment '���ԱС��������'
	partitioned by (`dt` string comment 'ͳ������')
	row format delimited fields terminated by '\t'
	null defined as ''
	location '/warehouse/tms/ods/ods_express_courier_complex_full';

-- employee_info
-- 13. Ա����
drop table if exists ods_employee_info_full;
create external table ods_employee_info_full(
	`id` bigint COMMENT 'Ա��ID',
	`username` string COMMENT '�û���',
	`password` string COMMENT '����',
	`real_name` string COMMENT '��ʵ����',
	`id_card` string COMMENT '���֤��',
	`phone` string COMMENT '�ֻ���',
	`birthday` string COMMENT '����',
	`gender` string COMMENT '�Ա�',
	`address` string COMMENT '��ַ',
	`employment_date` string COMMENT '��ְ����',
	`graduation_date` string COMMENT '��ְ����',
	`education` string COMMENT 'ѧ��',
	`position_type` string COMMENT '��λ���',
	`create_time` string COMMENT '����ʱ��',
	`update_time` string COMMENT '����ʱ��',
	`is_deleted` string COMMENT 'ɾ����ǣ�0:������ 1:���ã�'
) comment 'Ա����'
	partitioned by (`dt` string comment 'ͳ������')
	row format delimited fields terminated by '\t'
	null defined as ''
	location '/warehouse/tms/ods/ods_employee_info_full';

-- line_base_shift
-- 14. ��α�
drop table if exists ods_line_base_shift_full;
create external table ods_line_base_shift_full(
	`id` bigint COMMENT '���ID',
	`line_id` bigint COMMENT '��·ID',
	`start_time` string COMMENT '��ο�ʼʱ��',
	`driver1_emp_id` bigint COMMENT '��һ˾��',
	`driver2_emp_id` bigint COMMENT '�ڶ�˾��',
	`truck_id` bigint COMMENT '����',
	`pair_shift_id` bigint COMMENT '��԰��(ͬһ����һȥһ�ص���һ���)',
	`is_enabled` string COMMENT '״̬ 0������ 1������',
	`create_time` string COMMENT '����ʱ��',
	`update_time` string COMMENT '����ʱ��',
	`is_deleted` string COMMENT 'ɾ����ǣ�0:������ 1:���ã�'
) comment '��α�'
	partitioned by (`dt` string comment 'ͳ������')
	row format delimited fields terminated by '\t'
	null defined as ''
	location '/warehouse/tms/ods/ods_line_base_shift_full';

-- line_base_info
-- 15. ������·��
drop table if exists ods_line_base_info_full;
create external table ods_line_base_info_full(
	`id` bigint COMMENT '��·ID',
	`name` string COMMENT '��·����',
	`line_no` string COMMENT '��·���',
	`line_level` string COMMENT '��·����',
	`org_id` bigint COMMENT '��������',
	`transport_line_type_id` string COMMENT '��·����',
	`start_org_id` bigint COMMENT '��ʼ����ID',
	`start_org_name` string COMMENT '��ʼ��������',
	`end_org_id` bigint COMMENT 'Ŀ�����ID',
	`end_org_name` string COMMENT 'Ŀ���������',
	`pair_line_id` bigint COMMENT '�����·ID',
	`distance` decimal(10,2) COMMENT 'Ԥ�����',
	`cost` decimal(10,2) COMMENT 'ʵ�����',
	`estimated_time` bigint COMMENT 'Ԥ��ʱ�䣨���ӣ�',
	`status` string COMMENT '״̬ 0������ 1������',
	`create_time` string COMMENT '����ʱ��',
	`update_time` string COMMENT '����ʱ��',
	`is_deleted` string COMMENT 'ɾ����ǣ�0:������ 1:���ã�'
) comment '������·��'
	partitioned by (`dt` string comment 'ͳ������')
	row format delimited fields terminated by '\t'
	null defined as ''
	location '/warehouse/tms/ods/ods_line_base_info_full';

-- truck_driver
-- 16. ˾����Ϣ��
drop table if exists ods_truck_driver_full;
create external table ods_truck_driver_full(
	`id` bigint COMMENT '˾����ϢID',
	`emp_id` bigint COMMENT 'Ա��ID',
	`org_id` bigint COMMENT '��������ID',
	`team_id` bigint COMMENT '��������ID',
	`license_type` string COMMENT '׼�ݳ���',
	`init_license_date` string COMMENT '������֤����',
	`expire_date` string COMMENT '��Ч��ֹ����',
	`license_no` string COMMENT '��ʻ֤��',
	`license_picture_url` string COMMENT '��ʻ֤ͼƬ����',
	`is_enabled` tinyint COMMENT '״̬ 0������ 1������',
	`create_time` string COMMENT '����ʱ��',
	`update_time` string COMMENT '����ʱ��',
	`is_deleted` string COMMENT 'ɾ����ǣ�0:������ 1:���ã�'
) comment '˾����Ϣ��'
	partitioned by (`dt` string comment 'ͳ������')
	row format delimited fields terminated by '\t'
	null defined as ''
	location '/warehouse/tms/ods/ods_truck_driver_full';

-- truck_info
-- 17. ������Ϣ��
drop table if exists ods_truck_info_full;
create external table ods_truck_info_full(
	`id` bigint COMMENT '����ID',
	`team_id` bigint COMMENT '��������ID',
	`truck_no` string COMMENT '���ƺ���',
	`truck_model_id` string COMMENT '�ͺ�',
	`device_gps_id` string COMMENT 'GPS�豸ID',
	`engine_no` string COMMENT '����������',
	`license_registration_date` string COMMENT 'ע��ʱ��',
	`license_last_check_date` string COMMENT '����������',
	`license_expire_date` string COMMENT 'ʧЧ����',
	`picture_url` string COMMENT 'ͼƬ����',
	`is_enabled` tinyint COMMENT '״̬ 0������ 1������',
	`create_time` string COMMENT '����ʱ��',
	`update_time` string COMMENT '����ʱ��',
	`is_deleted` string COMMENT 'ɾ����ǣ�0:������ 1:���ã�'
) comment '������Ϣ��'
	partitioned by (`dt` string comment 'ͳ������')
	row format delimited fields terminated by '\t'
	null defined as ''
	location '/warehouse/tms/ods/ods_truck_info_full';

-- truck_model
-- 18. �����ͺű�
drop table if exists ods_truck_model_full;
create external table ods_truck_model_full(
	`id` bigint COMMENT '�ͺ�ID',
	`model_name` string COMMENT '�ͺ�����',
	`model_type` string COMMENT '�ͺ�����',
	`model_no` string COMMENT '�ͺű���',
	`brand` string COMMENT 'Ʒ��',
	`truck_weight` decimal(16,2) COMMENT '�����������֣�',
	`load_weight` decimal(16,2) COMMENT '����أ��֣�',
	`total_weight` decimal(16,2) COMMENT '���������֣�',
	`eev` string COMMENT '�ŷű�׼',
	`boxcar_len` decimal(16,2) COMMENT '���䳤��m��',
	`boxcar_wd` decimal(16,2) COMMENT '�����m��',
	`boxcar_hg` decimal(16,2) COMMENT '����ߣ�m��',
	`max_speed` bigint COMMENT '���ʱ�٣�ǧ��/ʱ��',
	`oil_vol` bigint COMMENT '�����ݻ�������',
	`create_time` string COMMENT '����ʱ��',
	`update_time` string COMMENT '����ʱ��',
	`is_deleted` string COMMENT 'ɾ����ǣ�0:������ 1:���ã�'
) comment '�����ͺű�'
	partitioned by (`dt` string comment 'ͳ������')
	row format delimited fields terminated by '\t'
	null defined as ''
	location '/warehouse/tms/ods/ods_truck_model_full';

-- truck_team
-- 19. ������Ϣ��
drop table if exists ods_truck_team_full;
create external table ods_truck_team_full(
	`id` bigint COMMENT '����ID',
	`name` string COMMENT '��������',
	`team_no` string COMMENT '���ӱ��',
	`org_id` bigint COMMENT '��������',
	`manager_emp_id` bigint COMMENT '������',
	`create_time` string COMMENT '����ʱ��',
	`update_time` string COMMENT '����ʱ��',
	`is_deleted` string COMMENT 'ɾ����ǣ�0:������ 1:���ã�'
) comment '������Ϣ��'
	partitioned by (`dt` string comment 'ͳ������')
	row format delimited fields terminated by '\t'
	null defined as ''
	location '/warehouse/tms/ods/ods_truck_team_full';


