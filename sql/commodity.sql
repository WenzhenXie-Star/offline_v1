create database commodity;

-- 1. 用户信息表
CREATE TABLE dim_user (
                          user_id string COMMENT '用户唯一标识（格式如U001）',
                          register_date DATE COMMENT '用户注册日期（格式：YYYY-MM-DD）',
                          is_new_user INT COMMENT '是否新用户：0-老用户（注册超过365天），1-新用户（注册未满365天）'
) COMMENT '用户维度表';

-- 2. 商品信息表
CREATE TABLE dim_product (
                             product_id string COMMENT '商品唯一标识（格式如P0001）',
                             product_name string COMMENT '商品名称（如“电子产品-黑色”）',
                             category_id string COMMENT '叶子类目ID（关联dim_category.category_id）',
                             price_bin string COMMENT '价格区间分档（硬编码值，如0-50、51-100等）',
                             goods_status string COMMENT '商品状态：上架-可售卖，下架-不可售卖'
) COMMENT '商品维度表';

-- 3. 类目表
CREATE TABLE dim_category (
                              category_id string COMMENT '类目唯一标识（格式如C1）',
                              category_name string COMMENT '类目名称（如“手机”）',
                              parent_category_id string COMMENT '父类目ID（根类目为ROOT）',
                              is_leaf INT COMMENT '是否为叶子类目：0-非叶子类目（有子类目），1-叶子类目（最末级）'
) COMMENT '商品类目维度表';

-- 4. 用户行为日志表
CREATE TABLE dwd_user_behavior_log (
                                       log_id INT COMMENT '日志记录唯一ID（自增主键）',
                                       user_id string COMMENT '用户ID（关联dim_user.user_id）',
                                       product_id string COMMENT '商品ID（关联dim_product.product_id）',
                                       behavior_type string COMMENT '行为类型：访问/收藏/加购/点击',
                                       behavior_time DATE COMMENT '行为发生时间（格式：YYYY-MM-DD HH:MM:SS）',
                                       terminal_type string COMMENT '终端类型：PC-电脑端，无线-手机/Pad端',
                                       stay_duration INT COMMENT '停留时长（单位：秒）',
                                       is_bounce INT COMMENT '是否跳出页面：0-未跳出（有后续操作），1-跳出（无后续操作）'
) COMMENT '用户行为日志明细表';

-- 5. 订单表
CREATE TABLE dwd_order (
                           order_id string COMMENT '订单唯一标识（格式如O00001）',
                           user_id string COMMENT '用户ID（关联dim_user.user_id）',
                           product_id string COMMENT '商品ID（关联dim_product.product_id）',
                           order_time DATE COMMENT '下单时间（格式：YYYY-MM-DD HH:MM:SS）',
                           payment_status string COMMENT '支付状态：已支付/未支付',
                           order_quantity INT COMMENT '下单数量（购买的商品件数）',
                           order_amount DECIMAL(10,2) COMMENT '下单金额（用户拍下时的总金额，未实际支付）',
                           terminal_type string COMMENT '下单终端：PC-电脑端，无线-手机/Pad端'
) COMMENT '订单明细表';

-- 6. 支付表
CREATE TABLE dwd_payment (
                             payment_id string COMMENT '支付记录唯一标识（格式如PAY00001）',
                             order_id string COMMENT '订单ID（关联dwd_order.order_id）',
                             user_id string COMMENT '用户ID（关联dim_user.user_id）',
                             payment_time DATE COMMENT '支付完成时间（格式：YYYY-MM-DD HH:MM:SS）',
                             payment_amount DECIMAL(10,2) COMMENT '实际pa_sum（扣除优惠后的金额）',
                             payment_channel string COMMENT '支付渠道：PC-电脑端支付，无线-手机端支付',
                             is_juhuasuan INT COMMENT '是否聚划算活动：0-否，1-是'
) COMMENT '支付明细表';

-- 7. 退款表
CREATE TABLE dwd_refund (
                            refund_id string COMMENT '退款记录唯一标识（格式如REF00001）',
                            order_id string COMMENT '订单ID（关联dwd_order.order_id）',
                            refund_amount DECIMAL(10,2) COMMENT '退款金额（实际退还给用户的金额）',
                            refund_time DATE COMMENT '退款处理时间（格式：YYYY-MM-DD HH:MM:SS）',
                            refund_type string COMMENT '退款类型：仅退款-不退货退钱，退货退款-退货并退款'
) COMMENT '退款明细表';

-- 商品基础信息表（分区按天）
CREATE TABLE IF NOT EXISTS dws_product_info (
                                                product_id STRING COMMENT '商品ID',
                                                product_name STRING COMMENT '商品名称',
                                                category_name STRING COMMENT '类目名称',
                                                price_bin STRING COMMENT '价格区间'
) COMMENT '商品基础信息表'
    PARTITIONED BY (dt STRING COMMENT '统计日期')
    STORED AS ORC
    TBLPROPERTIES ("orc.compress"="SNAPPY");

-- 访问行为统计表
CREATE TABLE IF NOT EXISTS dws_visit_stats (
                                               visitor_count INT COMMENT '访客数',
                                               page_view_count INT COMMENT '浏览量',
                                               avg_stay_duration DECIMAL(10,2) COMMENT '平均停留时长',
    bounce_rate DECIMAL(10,4) COMMENT '跳出率'
    ) COMMENT '访问行为统计表'
    PARTITIONED BY (product_id STRING, dt STRING)
    STORED AS ORC;

-- 收藏行为统计表
CREATE TABLE IF NOT EXISTS dws_favorite_stats (
    favorite_user_count INT COMMENT '收藏人数'
)
    PARTITIONED BY (product_id STRING, dt STRING)
    STORED AS ORC;

-- 加购行为统计表
CREATE TABLE IF NOT EXISTS dws_cart_stats (
                                              cart_item_count INT COMMENT '加购件数',
                                              cart_user_count INT COMMENT '加购人数'
)
    PARTITIONED BY (product_id STRING, dt STRING)
    STORED AS ORC;

-- 订单统计表
CREATE TABLE IF NOT EXISTS dws_order_stats (
                                               order_user_count INT COMMENT '下单买家数',
                                               order_item_count INT COMMENT '下单件数',
                                               order_amount DECIMAL(10,2) COMMENT '下单金额'
    )
    PARTITIONED BY (product_id STRING, dt STRING)
    STORED AS ORC;

-- 支付统计表
CREATE TABLE IF NOT EXISTS dws_payment_stats (
                                                 payment_user_count INT COMMENT '支付买家数',
                                                 payment_item_count INT COMMENT '支付件数',
                                                 payment_amount DECIMAL(10,2) COMMENT '支付金额',
    new_payment_user_count INT COMMENT '支付新买家数',
    old_payment_user_count INT COMMENT '支付老买家数',
    old_payment_amount DECIMAL(10,2) COMMENT '老买家支付金额'
    )
    PARTITIONED BY (product_id STRING, dt STRING)
    STORED AS ORC;

-- 退款统计表
CREATE TABLE IF NOT EXISTS dws_refund_stats (
                                                refund_amount DECIMAL(10,2) COMMENT '退款金额'
    )
    PARTITIONED BY (product_id STRING, dt STRING)
    STORED AS ORC;

-- 无线端访问统计表
CREATE TABLE IF NOT EXISTS dws_mobile_visit_stats (
    mobile_visitor_count INT COMMENT '无线端访客数'
)
    PARTITIONED BY (product_id STRING, dt STRING)
    STORED AS ORC;

-- 年度累计支付表
CREATE TABLE IF NOT EXISTS dws_yearly_payment (
                                                  yearly_payment_amount DECIMAL(10,2) COMMENT '年累计支付金额'
    )
    PARTITIONED BY (product_id STRING, year STRING)
    STORED AS ORC;



CREATE TABLE IF NOT EXISTS dws_product_analysis (
    -- 商品基础信息
                                                    product_id          STRING  COMMENT '商品ID',
                                                    product_name        STRING  COMMENT '商品名称',
                                                    category_name       STRING  COMMENT '类目名称',
                                                    price_bin           STRING  COMMENT '价格区间(如0-50)',
    -- 访问行为指标
                                                    visitor_count       INT     COMMENT '商品访客数',
                                                    page_view_count     INT     COMMENT '商品浏览量',
                                                    avg_stay_duration  DECIMAL(10,2) COMMENT '平均停留时长(秒)',
    bounce_rate        DECIMAL(10,4) COMMENT '详情页跳出率',
    -- 互动行为指标
    favorite_user_count INT     COMMENT '商品收藏人数',
    cart_item_count     INT     COMMENT '商品加购件数',
    cart_user_count     INT     COMMENT '商品加购人数',
    visit_to_favorite_rate DECIMAL(10,4) COMMENT '访问收藏转化率',
    visit_to_cart_rate    DECIMAL(10,4) COMMENT '访问加购转化率',
    -- 交易指标
    order_user_count    INT     COMMENT '下单买家数',
    order_item_count    INT     COMMENT '下单件数',
    order_amount       DECIMAL(10,2) COMMENT '下单金额',
    order_conversion_rate DECIMAL(10,4) COMMENT '下单转化率',
    -- 支付指标
    payment_user_count  INT     COMMENT '支付买家数',
    payment_item_count  INT     COMMENT '支付件数',
    payment_amount     DECIMAL(10,2) COMMENT '支付金额',
    payment_conversion_rate DECIMAL(10,4) COMMENT '支付转化率',
    new_payment_user_count INT COMMENT '支付新买家数',
    old_payment_user_count INT COMMENT '支付老买家数',
    old_payment_amount DECIMAL(10,2) COMMENT '老买家支付金额',
    -- 其他综合指标
    customer_unit_price DECIMAL(10,2) COMMENT '客单价(支付金额/支付买家数)',
    refund_amount      DECIMAL(10,2) COMMENT '成功退款退货金额',
    yearly_payment_amount DECIMAL(10,2) COMMENT '年累计支付金额',
    visitor_avg_value  DECIMAL(10,2) COMMENT '访客平均价值(支付金额/访客数)',
    competitiveness_score DECIMAL(10,2) COMMENT '竞争力评分',
    mobile_visitor_count INT    COMMENT '商品微详情访客数',
    -- 元数据
    update_time        TIMESTAMP COMMENT '数据更新时间'
    )
    COMMENT '商品维度综合分析结果表'
    PARTITIONED BY (dt STRING COMMENT '统计日期(YYYY-MM-DD)')
    STORED AS ORC
    TBLPROPERTIES (
    'orc.compress'='SNAPPY'
                  );

-- 启用动态分区
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.max.dynamic.partitions=1000;

-- 1. 写入商品基础信息（静态分区）
INSERT OVERWRITE TABLE dws_product_info PARTITION(dt='2025-03-31')
SELECT
    p.product_id,
    p.product_name,
    c.category_name,
    p.price_bin
FROM dim_product p
         LEFT JOIN dim_category c ON p.category_id = c.category_id
WHERE p.goods_status = '上架';

-- 2. 写入访问行为数据（动态分区）
INSERT OVERWRITE TABLE dws_visit_stats PARTITION(product_id, dt)
SELECT
    COUNT(DISTINCT user_id) AS visitor_count,
    COUNT(log_id) AS page_view_count,
    AVG(stay_duration) AS avg_stay_duration,
    AVG(is_bounce) AS bounce_rate,
    product_id,
    '2025-03-31' AS dt
FROM dwd_user_behavior_log
WHERE behavior_type = '访问'
GROUP BY product_id;

-- 3. 写入收藏行为数据
INSERT OVERWRITE TABLE dws_favorite_stats PARTITION(product_id, dt)
SELECT
    COUNT(DISTINCT user_id) AS favorite_user_count,
    product_id,
    '2025-03-31' AS dt
FROM dwd_user_behavior_log
WHERE behavior_type = '收藏'
GROUP BY product_id;

-- 4. 写入加购行为数据
INSERT OVERWRITE TABLE dws_cart_stats PARTITION(product_id, dt)
SELECT
    COUNT(log_id) AS cart_item_count,
    COUNT(DISTINCT user_id) AS cart_user_count,
    product_id,
    '2025-03-31' AS dt
FROM dwd_user_behavior_log
WHERE behavior_type = '加购'
GROUP BY product_id;

-- 5. 写入订单数据
INSERT OVERWRITE TABLE dws_order_stats PARTITION(product_id, dt)
SELECT
    COUNT(DISTINCT user_id) AS order_user_count,
    SUM(order_quantity) AS order_item_count,
    SUM(order_amount) AS order_amount,
    product_id,
    '2025-03-31' AS dt
FROM dwd_order
GROUP BY product_id;

-- 6. 写入支付数据
INSERT OVERWRITE TABLE dws_payment_stats PARTITION(product_id, dt)
SELECT
    COUNT(DISTINCT p.user_id) AS payment_user_count,
    SUM(o.order_quantity) AS payment_item_count,
    SUM(p.payment_amount) AS payment_amount,
    COUNT(DISTINCT CASE WHEN u.is_new_user = 1 THEN p.user_id END) AS new_payment_user_count,
    COUNT(DISTINCT CASE WHEN u.is_new_user = 0 THEN p.user_id END) AS old_payment_user_count,
    SUM(CASE WHEN u.is_new_user = 0 THEN p.payment_amount ELSE 0 END) AS old_payment_amount,
    o.product_id,
    '2025-03-31' AS dt
FROM dwd_payment p
         JOIN dwd_order o ON p.order_id = o.order_id
         LEFT JOIN dim_user u ON p.user_id = u.user_id
GROUP BY o.product_id;

-- 7. 写入退款数据
INSERT OVERWRITE TABLE dws_refund_stats PARTITION(product_id, dt)
SELECT
    SUM(r.refund_amount) AS refund_amount,
    o.product_id,
    '2025-03-31' AS dt
FROM dwd_refund r
         JOIN dwd_order o ON r.order_id = o.order_id
WHERE r.refund_type = '退货退款'
GROUP BY o.product_id;

-- 8. 写入无线端访问数据
INSERT OVERWRITE TABLE dws_mobile_visit_stats PARTITION(product_id, dt)
SELECT
    COUNT(DISTINCT user_id) AS mobile_visitor_count,
    product_id,
    '2025-03-31' AS dt
FROM dwd_user_behavior_log
WHERE behavior_type = '访问'
  AND terminal_type = '无线'
GROUP BY product_id;

-- 9. 写入年度累计支付数据
INSERT OVERWRITE TABLE dws_yearly_payment PARTITION(product_id, year)
SELECT
    SUM(p.payment_amount) AS yearly_payment_amount,
    o.product_id,
    YEAR('2025-03-31') AS year
FROM dwd_payment p
    JOIN dwd_order o ON p.order_id = o.order_id
WHERE YEAR(p.payment_time) = YEAR('2025-03-31')
GROUP BY o.product_id;



INSERT OVERWRITE TABLE dws_product_analysis PARTITION(dt='2025-03-31')
SELECT
    pi.product_id,
    pi.product_name,
    pi.category_name,
    pi.price_bin,
    COALESCE(vs.visitor_count, 0) AS visitor_count,
    COALESCE(vs.page_view_count, 0) AS page_view_count,
    ROUND(COALESCE(vs.avg_stay_duration, 0), 2) AS avg_stay_duration,
    ROUND(COALESCE(vs.bounce_rate, 0), 4) AS bounce_rate,
    COALESCE(fs.favorite_user_count, 0) AS favorite_user_count,
    COALESCE(cs.cart_item_count, 0) AS cart_item_count,
    COALESCE(cs.cart_user_count, 0) AS cart_user_count,
    ROUND(COALESCE(1.0 * fs.favorite_user_count / NULLIF(vs.visitor_count, 0), 0), 4) AS visit_to_favorite_rate,
    ROUND(COALESCE(1.0 * cs.cart_user_count / NULLIF(vs.visitor_count, 0), 0), 4) AS visit_to_cart_rate,
    COALESCE(os.order_user_count, 0) AS order_user_count,
    COALESCE(os.order_item_count, 0) AS order_item_count,
    COALESCE(os.order_amount, 0) AS order_amount,
    ROUND(COALESCE(1.0 * os.order_user_count / NULLIF(vs.visitor_count, 0), 0), 4) AS order_conversion_rate,
    COALESCE(ps.payment_user_count, 0) AS payment_user_count,
    COALESCE(ps.payment_item_count, 0) AS payment_item_count,
    COALESCE(ps.payment_amount, 0) AS payment_amount,
    ROUND(COALESCE(1.0 * ps.payment_user_count / NULLIF(os.order_user_count, 0), 0), 4) AS payment_conversion_rate,
    COALESCE(ps.new_payment_user_count, 0) AS new_payment_user_count,
    COALESCE(ps.old_payment_user_count, 0) AS old_payment_user_count,
    COALESCE(ps.old_payment_amount, 0) AS old_payment_amount,
    ROUND(COALESCE(ps.payment_amount / NULLIF(ps.payment_user_count, 0), 0), 2) AS customer_unit_price,
    COALESCE(rs.refund_amount, 0) AS refund_amount,
    COALESCE(yp.yearly_payment_amount, 0) AS yearly_payment_amount,
    ROUND(COALESCE(ps.payment_amount / NULLIF(vs.visitor_count, 0), 0), 2) AS visitor_avg_value,
    ROUND(
                    (COALESCE(ps.payment_amount, 0) * 0.5) +
                    (COALESCE(1.0 * ps.payment_user_count / NULLIF(vs.visitor_count, 0), 0) * 100 * 0.3) -
                    (COALESCE(rs.refund_amount, 0) * 0.2),
                    2
        ) AS competitiveness_score,
    COALESCE(mvs.mobile_visitor_count, 0) AS mobile_visitor_count,
    CURRENT_TIMESTAMP() AS update_time
FROM dws_product_info pi
         LEFT JOIN dws_visit_stats vs ON pi.product_id = vs.product_id AND pi.dt = vs.dt
         LEFT JOIN dws_favorite_stats fs ON pi.product_id = fs.product_id AND pi.dt = fs.dt
         LEFT JOIN dws_cart_stats cs ON pi.product_id = cs.product_id AND pi.dt = cs.dt
         LEFT JOIN dws_order_stats os ON pi.product_id = os.product_id AND pi.dt = os.dt
         LEFT JOIN dws_payment_stats ps ON pi.product_id = ps.product_id AND pi.dt = ps.dt
         LEFT JOIN dws_refund_stats rs ON pi.product_id = rs.product_id AND pi.dt = rs.dt
         LEFT JOIN dws_mobile_visit_stats mvs ON pi.product_id = mvs.product_id AND pi.dt = mvs.dt
         LEFT JOIN dws_yearly_payment yp ON pi.product_id = yp.product_id AND YEAR(pi.dt) = yp.year
WHERE pi.dt = '2025-03-31';
