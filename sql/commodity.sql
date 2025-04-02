create database commodity;

-- 1. �û���Ϣ��
CREATE TABLE dim_user (
                          user_id string COMMENT '�û�Ψһ��ʶ����ʽ��U001��',
                          register_date DATE COMMENT '�û�ע�����ڣ���ʽ��YYYY-MM-DD��',
                          is_new_user INT COMMENT '�Ƿ����û���0-���û���ע�ᳬ��365�죩��1-���û���ע��δ��365�죩'
) COMMENT '�û�ά�ȱ�';

-- 2. ��Ʒ��Ϣ��
CREATE TABLE dim_product (
                             product_id string COMMENT '��ƷΨһ��ʶ����ʽ��P0001��',
                             product_name string COMMENT '��Ʒ���ƣ��硰���Ӳ�Ʒ-��ɫ����',
                             category_id string COMMENT 'Ҷ����ĿID������dim_category.category_id��',
                             price_bin string COMMENT '�۸�����ֵ���Ӳ����ֵ����0-50��51-100�ȣ�',
                             goods_status string COMMENT '��Ʒ״̬���ϼ�-���������¼�-��������'
) COMMENT '��Ʒά�ȱ�';

-- 3. ��Ŀ��
CREATE TABLE dim_category (
                              category_id string COMMENT '��ĿΨһ��ʶ����ʽ��C1��',
                              category_name string COMMENT '��Ŀ���ƣ��硰�ֻ�����',
                              parent_category_id string COMMENT '����ĿID������ĿΪROOT��',
                              is_leaf INT COMMENT '�Ƿ�ΪҶ����Ŀ��0-��Ҷ����Ŀ��������Ŀ����1-Ҷ����Ŀ����ĩ����'
) COMMENT '��Ʒ��Ŀά�ȱ�';

-- 4. �û���Ϊ��־��
CREATE TABLE dwd_user_behavior_log (
                                       log_id INT COMMENT '��־��¼ΨһID������������',
                                       user_id string COMMENT '�û�ID������dim_user.user_id��',
                                       product_id string COMMENT '��ƷID������dim_product.product_id��',
                                       behavior_type string COMMENT '��Ϊ���ͣ�����/�ղ�/�ӹ�/���',
                                       behavior_time DATE COMMENT '��Ϊ����ʱ�䣨��ʽ��YYYY-MM-DD HH:MM:SS��',
                                       terminal_type string COMMENT '�ն����ͣ�PC-���Զˣ�����-�ֻ�/Pad��',
                                       stay_duration INT COMMENT 'ͣ��ʱ������λ���룩',
                                       is_bounce INT COMMENT '�Ƿ�����ҳ�棺0-δ�������к�����������1-�������޺���������'
) COMMENT '�û���Ϊ��־��ϸ��';

-- 5. ������
CREATE TABLE dwd_order (
                           order_id string COMMENT '����Ψһ��ʶ����ʽ��O00001��',
                           user_id string COMMENT '�û�ID������dim_user.user_id��',
                           product_id string COMMENT '��ƷID������dim_product.product_id��',
                           order_time DATE COMMENT '�µ�ʱ�䣨��ʽ��YYYY-MM-DD HH:MM:SS��',
                           payment_status string COMMENT '֧��״̬����֧��/δ֧��',
                           order_quantity INT COMMENT '�µ��������������Ʒ������',
                           order_amount DECIMAL(10,2) COMMENT '�µ����û�����ʱ���ܽ�δʵ��֧����',
                           terminal_type string COMMENT '�µ��նˣ�PC-���Զˣ�����-�ֻ�/Pad��'
) COMMENT '������ϸ��';

-- 6. ֧����
CREATE TABLE dwd_payment (
                             payment_id string COMMENT '֧����¼Ψһ��ʶ����ʽ��PAY00001��',
                             order_id string COMMENT '����ID������dwd_order.order_id��',
                             user_id string COMMENT '�û�ID������dim_user.user_id��',
                             payment_time DATE COMMENT '֧�����ʱ�䣨��ʽ��YYYY-MM-DD HH:MM:SS��',
                             payment_amount DECIMAL(10,2) COMMENT 'ʵ��pa_sum���۳��Żݺ�Ľ�',
                             payment_channel string COMMENT '֧��������PC-���Զ�֧��������-�ֻ���֧��',
                             is_juhuasuan INT COMMENT '�Ƿ�ۻ�����0-��1-��'
) COMMENT '֧����ϸ��';

-- 7. �˿��
CREATE TABLE dwd_refund (
                            refund_id string COMMENT '�˿��¼Ψһ��ʶ����ʽ��REF00001��',
                            order_id string COMMENT '����ID������dwd_order.order_id��',
                            refund_amount DECIMAL(10,2) COMMENT '�˿��ʵ���˻����û��Ľ�',
                            refund_time DATE COMMENT '�˿��ʱ�䣨��ʽ��YYYY-MM-DD HH:MM:SS��',
                            refund_type string COMMENT '�˿����ͣ����˿�-���˻���Ǯ���˻��˿�-�˻����˿�'
) COMMENT '�˿���ϸ��';

-- ��Ʒ������Ϣ���������죩
CREATE TABLE IF NOT EXISTS dws_product_info (
                                                product_id STRING COMMENT '��ƷID',
                                                product_name STRING COMMENT '��Ʒ����',
                                                category_name STRING COMMENT '��Ŀ����',
                                                price_bin STRING COMMENT '�۸�����'
) COMMENT '��Ʒ������Ϣ��'
    PARTITIONED BY (dt STRING COMMENT 'ͳ������')
    STORED AS ORC
    TBLPROPERTIES ("orc.compress"="SNAPPY");

-- ������Ϊͳ�Ʊ�
CREATE TABLE IF NOT EXISTS dws_visit_stats (
                                               visitor_count INT COMMENT '�ÿ���',
                                               page_view_count INT COMMENT '�����',
                                               avg_stay_duration DECIMAL(10,2) COMMENT 'ƽ��ͣ��ʱ��',
    bounce_rate DECIMAL(10,4) COMMENT '������'
    ) COMMENT '������Ϊͳ�Ʊ�'
    PARTITIONED BY (product_id STRING, dt STRING)
    STORED AS ORC;

-- �ղ���Ϊͳ�Ʊ�
CREATE TABLE IF NOT EXISTS dws_favorite_stats (
    favorite_user_count INT COMMENT '�ղ�����'
)
    PARTITIONED BY (product_id STRING, dt STRING)
    STORED AS ORC;

-- �ӹ���Ϊͳ�Ʊ�
CREATE TABLE IF NOT EXISTS dws_cart_stats (
                                              cart_item_count INT COMMENT '�ӹ�����',
                                              cart_user_count INT COMMENT '�ӹ�����'
)
    PARTITIONED BY (product_id STRING, dt STRING)
    STORED AS ORC;

-- ����ͳ�Ʊ�
CREATE TABLE IF NOT EXISTS dws_order_stats (
                                               order_user_count INT COMMENT '�µ������',
                                               order_item_count INT COMMENT '�µ�����',
                                               order_amount DECIMAL(10,2) COMMENT '�µ����'
    )
    PARTITIONED BY (product_id STRING, dt STRING)
    STORED AS ORC;

-- ֧��ͳ�Ʊ�
CREATE TABLE IF NOT EXISTS dws_payment_stats (
                                                 payment_user_count INT COMMENT '֧�������',
                                                 payment_item_count INT COMMENT '֧������',
                                                 payment_amount DECIMAL(10,2) COMMENT '֧�����',
    new_payment_user_count INT COMMENT '֧���������',
    old_payment_user_count INT COMMENT '֧���������',
    old_payment_amount DECIMAL(10,2) COMMENT '�����֧�����'
    )
    PARTITIONED BY (product_id STRING, dt STRING)
    STORED AS ORC;

-- �˿�ͳ�Ʊ�
CREATE TABLE IF NOT EXISTS dws_refund_stats (
                                                refund_amount DECIMAL(10,2) COMMENT '�˿���'
    )
    PARTITIONED BY (product_id STRING, dt STRING)
    STORED AS ORC;

-- ���߶˷���ͳ�Ʊ�
CREATE TABLE IF NOT EXISTS dws_mobile_visit_stats (
    mobile_visitor_count INT COMMENT '���߶˷ÿ���'
)
    PARTITIONED BY (product_id STRING, dt STRING)
    STORED AS ORC;

-- ����ۼ�֧����
CREATE TABLE IF NOT EXISTS dws_yearly_payment (
                                                  yearly_payment_amount DECIMAL(10,2) COMMENT '���ۼ�֧�����'
    )
    PARTITIONED BY (product_id STRING, year STRING)
    STORED AS ORC;



CREATE TABLE IF NOT EXISTS dws_product_analysis (
    -- ��Ʒ������Ϣ
                                                    product_id          STRING  COMMENT '��ƷID',
                                                    product_name        STRING  COMMENT '��Ʒ����',
                                                    category_name       STRING  COMMENT '��Ŀ����',
                                                    price_bin           STRING  COMMENT '�۸�����(��0-50)',
    -- ������Ϊָ��
                                                    visitor_count       INT     COMMENT '��Ʒ�ÿ���',
                                                    page_view_count     INT     COMMENT '��Ʒ�����',
                                                    avg_stay_duration  DECIMAL(10,2) COMMENT 'ƽ��ͣ��ʱ��(��)',
    bounce_rate        DECIMAL(10,4) COMMENT '����ҳ������',
    -- ������Ϊָ��
    favorite_user_count INT     COMMENT '��Ʒ�ղ�����',
    cart_item_count     INT     COMMENT '��Ʒ�ӹ�����',
    cart_user_count     INT     COMMENT '��Ʒ�ӹ�����',
    visit_to_favorite_rate DECIMAL(10,4) COMMENT '�����ղ�ת����',
    visit_to_cart_rate    DECIMAL(10,4) COMMENT '���ʼӹ�ת����',
    -- ����ָ��
    order_user_count    INT     COMMENT '�µ������',
    order_item_count    INT     COMMENT '�µ�����',
    order_amount       DECIMAL(10,2) COMMENT '�µ����',
    order_conversion_rate DECIMAL(10,4) COMMENT '�µ�ת����',
    -- ֧��ָ��
    payment_user_count  INT     COMMENT '֧�������',
    payment_item_count  INT     COMMENT '֧������',
    payment_amount     DECIMAL(10,2) COMMENT '֧�����',
    payment_conversion_rate DECIMAL(10,4) COMMENT '֧��ת����',
    new_payment_user_count INT COMMENT '֧���������',
    old_payment_user_count INT COMMENT '֧���������',
    old_payment_amount DECIMAL(10,2) COMMENT '�����֧�����',
    -- �����ۺ�ָ��
    customer_unit_price DECIMAL(10,2) COMMENT '�͵���(֧�����/֧�������)',
    refund_amount      DECIMAL(10,2) COMMENT '�ɹ��˿��˻����',
    yearly_payment_amount DECIMAL(10,2) COMMENT '���ۼ�֧�����',
    visitor_avg_value  DECIMAL(10,2) COMMENT '�ÿ�ƽ����ֵ(֧�����/�ÿ���)',
    competitiveness_score DECIMAL(10,2) COMMENT '����������',
    mobile_visitor_count INT    COMMENT '��Ʒ΢����ÿ���',
    -- Ԫ����
    update_time        TIMESTAMP COMMENT '���ݸ���ʱ��'
    )
    COMMENT '��Ʒά���ۺϷ��������'
    PARTITIONED BY (dt STRING COMMENT 'ͳ������(YYYY-MM-DD)')
    STORED AS ORC
    TBLPROPERTIES (
    'orc.compress'='SNAPPY'
                  );

-- ���ö�̬����
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.max.dynamic.partitions=1000;

-- 1. д����Ʒ������Ϣ����̬������
INSERT OVERWRITE TABLE dws_product_info PARTITION(dt='2025-03-31')
SELECT
    p.product_id,
    p.product_name,
    c.category_name,
    p.price_bin
FROM dim_product p
         LEFT JOIN dim_category c ON p.category_id = c.category_id
WHERE p.goods_status = '�ϼ�';

-- 2. д�������Ϊ���ݣ���̬������
INSERT OVERWRITE TABLE dws_visit_stats PARTITION(product_id, dt)
SELECT
    COUNT(DISTINCT user_id) AS visitor_count,
    COUNT(log_id) AS page_view_count,
    AVG(stay_duration) AS avg_stay_duration,
    AVG(is_bounce) AS bounce_rate,
    product_id,
    '2025-03-31' AS dt
FROM dwd_user_behavior_log
WHERE behavior_type = '����'
GROUP BY product_id;

-- 3. д���ղ���Ϊ����
INSERT OVERWRITE TABLE dws_favorite_stats PARTITION(product_id, dt)
SELECT
    COUNT(DISTINCT user_id) AS favorite_user_count,
    product_id,
    '2025-03-31' AS dt
FROM dwd_user_behavior_log
WHERE behavior_type = '�ղ�'
GROUP BY product_id;

-- 4. д��ӹ���Ϊ����
INSERT OVERWRITE TABLE dws_cart_stats PARTITION(product_id, dt)
SELECT
    COUNT(log_id) AS cart_item_count,
    COUNT(DISTINCT user_id) AS cart_user_count,
    product_id,
    '2025-03-31' AS dt
FROM dwd_user_behavior_log
WHERE behavior_type = '�ӹ�'
GROUP BY product_id;

-- 5. д�붩������
INSERT OVERWRITE TABLE dws_order_stats PARTITION(product_id, dt)
SELECT
    COUNT(DISTINCT user_id) AS order_user_count,
    SUM(order_quantity) AS order_item_count,
    SUM(order_amount) AS order_amount,
    product_id,
    '2025-03-31' AS dt
FROM dwd_order
GROUP BY product_id;

-- 6. д��֧������
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

-- 7. д���˿�����
INSERT OVERWRITE TABLE dws_refund_stats PARTITION(product_id, dt)
SELECT
    SUM(r.refund_amount) AS refund_amount,
    o.product_id,
    '2025-03-31' AS dt
FROM dwd_refund r
         JOIN dwd_order o ON r.order_id = o.order_id
WHERE r.refund_type = '�˻��˿�'
GROUP BY o.product_id;

-- 8. д�����߶˷�������
INSERT OVERWRITE TABLE dws_mobile_visit_stats PARTITION(product_id, dt)
SELECT
    COUNT(DISTINCT user_id) AS mobile_visitor_count,
    product_id,
    '2025-03-31' AS dt
FROM dwd_user_behavior_log
WHERE behavior_type = '����'
  AND terminal_type = '����'
GROUP BY product_id;

-- 9. д������ۼ�֧������
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
