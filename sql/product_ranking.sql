create database product_ranking;
use product_ranking;

CREATE TABLE `ads_price_power_alert` (
                                         `dt` date COMMENT '统计日期',
                                         `product_id` bigint COMMENT '商品ID',
                                         `product_name` string COMMENT '商品名称',
                                         `alert_type` string COMMENT '预警类型(价格力/商品力)',
                                         `alert_reason` string COMMENT '预警原因',
                                         `market_avg_conversion_rate` decimal(10,4) COMMENT '市场平均转化率',
                                         `product_conversion_rate` decimal(10,4) COMMENT '商品转化率',
                                         `price_power_star` decimal(3,1) COMMENT '价格力星级',
                                         `etl_date` date COMMENT 'ETL日期'
) COMMENT '价格力商品预警表';


CREATE TABLE `ads_product_ranking` (
                                       `dt` date COMMENT '统计日期',
                                       `time_dimension` string COMMENT '时间维度(7天/30天/日/周/月)',
                                       `product_id` string COMMENT '商品ID',
                                       `product_name` string COMMENT '商品名称',
                                       `category_id` bigint COMMENT '商品类目ID',
                                       `category_name` string COMMENT '商品类目名称',
                                       `rank_type` string COMMENT '排名类型(销售额/销量/转化率)',
                                       `rank_value` decimal(18,2) COMMENT '排名值',
                                       `rank_position` int COMMENT '排名位置',
                                       `payment_amount` decimal(18,2)  COMMENT '支付金额',
                                       `payment_quantity` bigint  COMMENT '支付件数',
                                       `visit_count` bigint COMMENT '访客数',
                                       `conversion_rate` decimal(10,4) COMMENT '支付转化率',
                                       `price_power_level` string COMMENT '价格力等级',
                                       `price_power_star` decimal(3,1) COMMENT '价格力星级',
                                       `etl_date` date COMMENT 'ETL日期'
) COMMENT '商品排行表';


SELECT
    ROW_NUMBER() OVER (ORDER BY rank_value desc),  -- 从1开始的自增数字字段
        dt,
    product_name,
    category_name,
    rank_value,
    rank_position
FROM product_ranking.ads_product_ranking
WHERE time_dimension = '月'
  AND rank_type = '销售额'
ORDER BY rank_value DESC;

CREATE TABLE `dim_product` (
                               `product_id` bigint COMMENT '商品ID',
                               `product_name` string COMMENT '商品名称',
                               `category_id` bigint COMMENT '商品类目ID',
                               `category_name` string COMMENT '商品类目名称',
                               `price` decimal(18,2) COMMENT '商品价格',
                               `cost` decimal(18,2) COMMENT '商品成本',
                               `create_time` date COMMENT '商品创建日期',
                               `modify_time` date COMMENT '商品修改日期',
                               `is_price_power_product` int COMMENT '是否价格力商品',
                               `price_power_level` string COMMENT '价格力等级(优秀/良好/较差)',
                               `etl_date` date COMMENT 'ETL日期'
) COMMENT '商品维度表';


CREATE TABLE `dim_product_sku` (
                                   `sku_id` bigint COMMENT 'SKU ID',
                                   `product_id` bigint COMMENT '商品ID',
                                   `sku_attrs` string COMMENT 'SKU属性(如颜色分类等)',
                                   `stock_quantity` int COMMENT '当前库存量',
                                   `etl_date` date COMMENT 'ETL日期'
)  COMMENT '商品SKU维度表';


CREATE TABLE `dws_product_daily` (
                                     `dt` date COMMENT '统计日期',
                                     `product_id` bigint COMMENT '商品ID',
                                     `visit_count` bigint COMMENT '访客数',
                                     `page_view_count` bigint COMMENT '浏览量',
                                     `cart_count` bigint COMMENT '加购数',
                                     `payment_user_count` bigint COMMENT '支付买家数',
                                     `payment_amount` decimal(18,2) COMMENT '支付金额',
                                     `payment_quantity` bigint COMMENT '支付件数',
                                     `refund_amount` decimal(18,2) COMMENT '退款金额',
                                     `refund_quantity` bigint COMMENT '退款件数',
                                     `conversion_rate` decimal(10,4) COMMENT '支付转化率(支付买家数/访客数)',
                                     `price_power_star` decimal(3,1) COMMENT '价格力星级',
                                     `product_power_score` decimal(5,2) COMMENT '商品力得分',
                                     `etl_date` date COMMENT 'ETL日期'
)COMMENT '商品日汇总表';


CREATE TABLE `dws_product_search_term_daily` (
                                                 `dt` date COMMENT '统计日期',
                                                 `product_id` bigint COMMENT '商品ID',
                                                 `search_term` string COMMENT '搜索词',
                                                 `visit_count` bigint COMMENT '访客数',
                                                 `etl_date` date COMMENT 'ETL日期'
) COMMENT '商品搜索词日统计表';


CREATE TABLE `dws_product_sku_daily` (
                                         `dt` date COMMENT '统计日期',
                                         `product_id` bigint COMMENT '商品ID',
                                         `sku_id` bigint COMMENT 'SKU ID',
                                         `payment_quantity` bigint COMMENT '支付件数',
                                         `payment_amount` decimal(18,2) COMMENT '支付金额',
                                         `refund_quantity` bigint COMMENT '退款件数',
                                         `refund_amount` decimal(18,2) COMMENT '退款金额',
                                         `etl_date` date COMMENT 'ETL日期'
)COMMENT '商品SKU日销售表';


CREATE TABLE `dws_product_traffic_source_daily` (
                                                    `dt` date COMMENT '统计日期',
                                                    `product_id` bigint COMMENT '商品ID',
                                                    `traffic_source` string COMMENT '流量来源(手淘搜索/效果广告等)',
                                                    `visit_count` bigint  COMMENT '访客数',
                                                    `payment_user_count` bigint  COMMENT '支付买家数',
                                                    `conversion_rate` decimal(10,4) COMMENT '支付转化率',
                                                    `etl_date` date COMMENT 'ETL日期'
)COMMENT '商品流量来源日统计表';



