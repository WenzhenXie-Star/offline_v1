create database product_ranking;
use product_ranking;

CREATE TABLE `ads_price_power_alert` (
                                         `dt` date COMMENT 'ͳ������',
                                         `product_id` bigint COMMENT '��ƷID',
                                         `product_name` string COMMENT '��Ʒ����',
                                         `alert_type` string COMMENT 'Ԥ������(�۸���/��Ʒ��)',
                                         `alert_reason` string COMMENT 'Ԥ��ԭ��',
                                         `market_avg_conversion_rate` decimal(10,4) COMMENT '�г�ƽ��ת����',
                                         `product_conversion_rate` decimal(10,4) COMMENT '��Ʒת����',
                                         `price_power_star` decimal(3,1) COMMENT '�۸����Ǽ�',
                                         `etl_date` date COMMENT 'ETL����'
) COMMENT '�۸�����ƷԤ����';


CREATE TABLE `ads_product_ranking` (
                                       `dt` date COMMENT 'ͳ������',
                                       `time_dimension` string COMMENT 'ʱ��ά��(7��/30��/��/��/��)',
                                       `product_id` string COMMENT '��ƷID',
                                       `product_name` string COMMENT '��Ʒ����',
                                       `category_id` bigint COMMENT '��Ʒ��ĿID',
                                       `category_name` string COMMENT '��Ʒ��Ŀ����',
                                       `rank_type` string COMMENT '��������(���۶�/����/ת����)',
                                       `rank_value` decimal(18,2) COMMENT '����ֵ',
                                       `rank_position` int COMMENT '����λ��',
                                       `payment_amount` decimal(18,2)  COMMENT '֧�����',
                                       `payment_quantity` bigint  COMMENT '֧������',
                                       `visit_count` bigint COMMENT '�ÿ���',
                                       `conversion_rate` decimal(10,4) COMMENT '֧��ת����',
                                       `price_power_level` string COMMENT '�۸����ȼ�',
                                       `price_power_star` decimal(3,1) COMMENT '�۸����Ǽ�',
                                       `etl_date` date COMMENT 'ETL����'
) COMMENT '��Ʒ���б�';


SELECT
    ROW_NUMBER() OVER (ORDER BY rank_value desc),  -- ��1��ʼ�����������ֶ�
        dt,
    product_name,
    category_name,
    rank_value,
    rank_position
FROM product_ranking.ads_product_ranking
WHERE time_dimension = '��'
  AND rank_type = '���۶�'
ORDER BY rank_value DESC;

CREATE TABLE `dim_product` (
                               `product_id` bigint COMMENT '��ƷID',
                               `product_name` string COMMENT '��Ʒ����',
                               `category_id` bigint COMMENT '��Ʒ��ĿID',
                               `category_name` string COMMENT '��Ʒ��Ŀ����',
                               `price` decimal(18,2) COMMENT '��Ʒ�۸�',
                               `cost` decimal(18,2) COMMENT '��Ʒ�ɱ�',
                               `create_time` date COMMENT '��Ʒ��������',
                               `modify_time` date COMMENT '��Ʒ�޸�����',
                               `is_price_power_product` int COMMENT '�Ƿ�۸�����Ʒ',
                               `price_power_level` string COMMENT '�۸����ȼ�(����/����/�ϲ�)',
                               `etl_date` date COMMENT 'ETL����'
) COMMENT '��Ʒά�ȱ�';


CREATE TABLE `dim_product_sku` (
                                   `sku_id` bigint COMMENT 'SKU ID',
                                   `product_id` bigint COMMENT '��ƷID',
                                   `sku_attrs` string COMMENT 'SKU����(����ɫ�����)',
                                   `stock_quantity` int COMMENT '��ǰ�����',
                                   `etl_date` date COMMENT 'ETL����'
)  COMMENT '��ƷSKUά�ȱ�';


CREATE TABLE `dws_product_daily` (
                                     `dt` date COMMENT 'ͳ������',
                                     `product_id` bigint COMMENT '��ƷID',
                                     `visit_count` bigint COMMENT '�ÿ���',
                                     `page_view_count` bigint COMMENT '�����',
                                     `cart_count` bigint COMMENT '�ӹ���',
                                     `payment_user_count` bigint COMMENT '֧�������',
                                     `payment_amount` decimal(18,2) COMMENT '֧�����',
                                     `payment_quantity` bigint COMMENT '֧������',
                                     `refund_amount` decimal(18,2) COMMENT '�˿���',
                                     `refund_quantity` bigint COMMENT '�˿����',
                                     `conversion_rate` decimal(10,4) COMMENT '֧��ת����(֧�������/�ÿ���)',
                                     `price_power_star` decimal(3,1) COMMENT '�۸����Ǽ�',
                                     `product_power_score` decimal(5,2) COMMENT '��Ʒ���÷�',
                                     `etl_date` date COMMENT 'ETL����'
)COMMENT '��Ʒ�ջ��ܱ�';


CREATE TABLE `dws_product_search_term_daily` (
                                                 `dt` date COMMENT 'ͳ������',
                                                 `product_id` bigint COMMENT '��ƷID',
                                                 `search_term` string COMMENT '������',
                                                 `visit_count` bigint COMMENT '�ÿ���',
                                                 `etl_date` date COMMENT 'ETL����'
) COMMENT '��Ʒ��������ͳ�Ʊ�';


CREATE TABLE `dws_product_sku_daily` (
                                         `dt` date COMMENT 'ͳ������',
                                         `product_id` bigint COMMENT '��ƷID',
                                         `sku_id` bigint COMMENT 'SKU ID',
                                         `payment_quantity` bigint COMMENT '֧������',
                                         `payment_amount` decimal(18,2) COMMENT '֧�����',
                                         `refund_quantity` bigint COMMENT '�˿����',
                                         `refund_amount` decimal(18,2) COMMENT '�˿���',
                                         `etl_date` date COMMENT 'ETL����'
)COMMENT '��ƷSKU�����۱�';


CREATE TABLE `dws_product_traffic_source_daily` (
                                                    `dt` date COMMENT 'ͳ������',
                                                    `product_id` bigint COMMENT '��ƷID',
                                                    `traffic_source` string COMMENT '������Դ(��������/Ч������)',
                                                    `visit_count` bigint  COMMENT '�ÿ���',
                                                    `payment_user_count` bigint  COMMENT '֧�������',
                                                    `conversion_rate` decimal(10,4) COMMENT '֧��ת����',
                                                    `etl_date` date COMMENT 'ETL����'
)COMMENT '��Ʒ������Դ��ͳ�Ʊ�';



