-- 钩沉 NL2SQL 测试数据库初始化脚本
-- 创建电商零售领域核心表

-- 创建数据库
DROP DATABASE IF EXISTS gouchen;
CREATE DATABASE gouchen CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
USE gouchen;

-- 1. 日期维度表
CREATE TABLE dim_date (
    date_key INT PRIMARY KEY COMMENT '日期键YYYYMMDD',
    date DATE NOT NULL COMMENT '标准日期',
    year INT COMMENT '年份',
    month INT COMMENT '月份',
    day INT COMMENT '日',
    quarter INT COMMENT '季度',
    week_of_year INT COMMENT '年内第几周',
    is_weekend BOOLEAN COMMENT '是否周末'
) COMMENT='日期维度表';

-- 2. 地区维度表
CREATE TABLE dim_region (
    region_code VARCHAR(20) PRIMARY KEY COMMENT '地区编码',
    region_name VARCHAR(100) COMMENT '地区名称',
    province VARCHAR(50) COMMENT '省份',
    city VARCHAR(50) COMMENT '城市',
    region_level INT COMMENT '地区级别1=省2=市3=区'
) COMMENT='地区维度表';

-- 3. 商品类目表
CREATE TABLE dim_category (
    category_id VARCHAR(50) PRIMARY KEY COMMENT '类目ID',
    category_name VARCHAR(100) COMMENT '类目名称',
    parent_id VARCHAR(50) COMMENT '父类目ID',
    category_level INT COMMENT '类目级别'
) COMMENT='商品类目表';

-- 4. 商品维度表
CREATE TABLE dim_product (
    product_id VARCHAR(50) PRIMARY KEY COMMENT '商品ID',
    product_name VARCHAR(200) COMMENT '商品名称',
    category_id VARCHAR(50) COMMENT '类目ID',
    price DECIMAL(10,2) COMMENT '售价'
) COMMENT='商品维度表';

-- 5. 订单事实表
CREATE TABLE fact_order (
    order_id VARCHAR(50) PRIMARY KEY COMMENT '订单ID',
    order_date DATE COMMENT '订单日期',
    region_code VARCHAR(20) COMMENT '地区编码',
    order_amount DECIMAL(12,2) COMMENT '订单金额',
    paid_amount DECIMAL(12,2) COMMENT '实付金额',
    order_status VARCHAR(20) COMMENT '订单状态'
) COMMENT='订单事实表';

-- 6. 订单明细表
CREATE TABLE fact_order_item (
    item_id VARCHAR(50) PRIMARY KEY COMMENT '明细ID',
    order_id VARCHAR(50) COMMENT '订单ID',
    product_id VARCHAR(50) COMMENT '商品ID',
    quantity INT COMMENT '数量',
    item_amount DECIMAL(12,2) COMMENT '明细金额'
) COMMENT='订单明细表';

-- 插入日期数据（2024年1月-2024年12月）
INSERT INTO dim_date VALUES
(20240101, '2024-01-01', 2024, 1, 1, 1, 1, FALSE),
(20240115, '2024-01-15', 2024, 1, 15, 1, 3, FALSE),
(20240201, '2024-02-01', 2024, 2, 1, 1, 5, FALSE),
(20240215, '2024-02-15', 2024, 2, 15, 1, 7, FALSE),
(20240301, '2024-03-01', 2024, 3, 1, 1, 9, FALSE),
(20240315, '2024-03-15', 2024, 3, 15, 1, 11, FALSE),
(20240401, '2024-04-01', 2024, 4, 1, 2, 14, FALSE),
(20240415, '2024-04-15', 2024, 4, 15, 2, 16, FALSE),
(20240501, '2024-05-01', 2024, 5, 1, 2, 18, FALSE),
(20240515, '2024-05-15', 2024, 5, 15, 2, 20, FALSE),
(20240601, '2024-06-01', 2024, 6, 1, 2, 22, FALSE),
(20240615, '2024-06-15', 2024, 6, 15, 2, 24, FALSE),
(20240701, '2024-07-01', 2024, 7, 1, 3, 27, FALSE),
(20240715, '2024-07-15', 2024, 7, 15, 3, 29, FALSE),
(20240801, '2024-08-01', 2024, 8, 1, 3, 31, FALSE),
(20240815, '2024-08-15', 2024, 8, 15, 3, 33, FALSE),
(20240901, '2024-09-01', 2024, 9, 1, 3, 36, FALSE),
(20240915, '2024-09-15', 2024, 9, 15, 3, 38, FALSE),
(20241001, '2024-10-01', 2024, 10, 1, 4, 40, FALSE),
(20241015, '2024-10-15', 2024, 10, 15, 4, 42, FALSE),
(20241101, '2024-11-01', 2024, 11, 1, 4, 44, FALSE),
(20241115, '2024-11-15', 2024, 11, 15, 4, 46, FALSE),
(20241201, '2024-12-01', 2024, 12, 1, 4, 48, FALSE),
(20241215, '2024-12-15', 2024, 12, 15, 4, 50, FALSE);

-- 插入地区数据
INSERT INTO dim_region VALUES
('EAST', '华东区', '华东', '上海', 1),
('EAST_JS', '江苏省', '华东', '南京', 2),
('EAST_ZJ', '浙江省', '华东', '杭州', 2),
('EAST_SH', '上海市', '华东', '上海', 2),
('SOUTH', '华南区', '华南', '广州', 1),
('SOUTH_GD', '广东省', '华南', '广州', 2),
('SOUTH_FJ', '福建省', '华南', '福州', 2),
('NORTH', '华北区', '华北', '北京', 1),
('NORTH_BJ', '北京市', '华北', '北京', 2),
('NORTH_TJ', '天津市', '华北', '天津', 2);

-- 插入类目数据
INSERT INTO dim_category VALUES
('CAT001', '手机数码', NULL, 1),
('CAT002', '智能手机', 'CAT001', 2),
('CAT003', '平板电脑', 'CAT001', 2),
('CAT004', '电脑办公', NULL, 1),
('CAT005', '笔记本电脑', 'CAT004', 2),
('CAT006', '台式机', 'CAT004', 2),
('CAT007', '家用电器', NULL, 1),
('CAT008', '空调', 'CAT007', 2),
('CAT009', '冰箱', 'CAT007', 2),
('CAT010', '洗衣机', 'CAT007', 2);

-- 插入商品数据
INSERT INTO dim_product VALUES
('P001', 'iPhone 15 Pro 128GB', 'CAT002', 7999.00),
('P002', 'iPhone 15 256GB', 'CAT002', 6999.00),
('P003', '华为Mate 60 Pro', 'CAT002', 6999.00),
('P004', '小米14 12GB+256GB', 'CAT002', 3999.00),
('P005', 'iPad Pro 11英寸', 'CAT003', 6799.00),
('P006', '华为MatePad Pro', 'CAT003', 4499.00),
('P007', 'MacBook Pro 14英寸', 'CAT005', 14999.00),
('P008', '联想ThinkPad X1', 'CAT005', 9999.00),
('P009', '戴尔XPS 13', 'CAT005', 8999.00),
('P010', '小米空调 1.5匹', 'CAT008', 2499.00),
('P011', '格力空调 1.5匹', 'CAT008', 3299.00),
('P012', '海尔冰箱 500L', 'CAT009', 3999.00),
('P013', '西门子冰箱 450L', 'CAT009', 5999.00),
('P014', '小天鹅洗衣机 10KG', 'CAT010', 1999.00),
('P015', '海尔洗衣机 10KG', 'CAT010', 2499.00);

-- 插入订单数据（2024年3月-4月，模拟上个月的数据）
INSERT INTO fact_order VALUES
('O001', '2024-03-01', 'EAST_SH', 7999.00, 7999.00, '已完成'),
('O002', '2024-03-02', 'EAST_JS', 6999.00, 6999.00, '已完成'),
('O003', '2024-03-05', 'SOUTH_GD', 3999.00, 3999.00, '已完成'),
('O004', '2024-03-08', 'EAST_ZJ', 14999.00, 14999.00, '已完成'),
('O005', '2024-03-10', 'NORTH_BJ', 9999.00, 9999.00, '已完成'),
('O006', '2024-03-15', 'EAST_SH', 2499.00, 2499.00, '已完成'),
('O007', '2024-03-18', 'SOUTH_GD', 3299.00, 3299.00, '已完成'),
('O008', '2024-03-20', 'EAST_JS', 3999.00, 3999.00, '已完成'),
('O009', '2024-03-25', 'NORTH_BJ', 5999.00, 5999.00, '已完成'),
('O010', '2024-03-28', 'EAST_ZJ', 6999.00, 6999.00, '已完成'),
('O011', '2024-04-01', 'EAST_SH', 6799.00, 6799.00, '已完成'),
('O012', '2024-04-03', 'SOUTH_GD', 4499.00, 4499.00, '已完成'),
('O013', '2024-04-05', 'EAST_JS', 8999.00, 8999.00, '已完成'),
('O014', '2024-04-08', 'NORTH_BJ', 1999.00, 1999.00, '已完成'),
('O015', '2024-04-10', 'EAST_ZJ', 2499.00, 2499.00, '已完成'),
('O016', '2024-04-12', 'SOUTH_FJ', 3999.00, 3999.00, '已完成'),
('O017', '2024-04-15', 'EAST_SH', 7999.00, 7999.00, '已完成'),
('O018', '2024-04-18', 'EAST_JS', 3299.00, 3299.00, '已完成'),
('O019', '2024-04-20', 'NORTH_TJ', 5999.00, 5999.00, '已完成'),
('O020', '2024-04-22', 'SOUTH_GD', 6999.00, 6999.00, '已完成'),
('O021', '2024-04-25', 'EAST_ZJ', 9999.00, 9999.00, '已完成'),
('O022', '2024-04-28', 'EAST_SH', 3999.00, 3999.00, '已完成');

-- 插入订单明细数据
INSERT INTO fact_order_item VALUES
('I001', 'O001', 'P001', 1, 7999.00),
('I002', 'O002', 'P002', 1, 6999.00),
('I003', 'O003', 'P004', 1, 3999.00),
('I004', 'O004', 'P007', 1, 14999.00),
('I005', 'O005', 'P008', 1, 9999.00),
('I006', 'O006', 'P010', 1, 2499.00),
('I007', 'O007', 'P011', 1, 3299.00),
('I008', 'O008', 'P004', 1, 3999.00),
('I009', 'O009', 'P013', 1, 5999.00),
('I010', 'O010', 'P003', 1, 6999.00),
('I011', 'O011', 'P005', 1, 6799.00),
('I012', 'O012', 'P006', 1, 4499.00),
('I013', 'O013', 'P009', 1, 8999.00),
('I014', 'O014', 'P014', 1, 1999.00),
('I015', 'O015', 'P015', 1, 2499.00),
('I016', 'O016', 'P004', 1, 3999.00),
('I017', 'O017', 'P001', 1, 7999.00),
('I018', 'O018', 'P011', 1, 3299.00),
('I019', 'O019', 'P013', 1, 5999.00),
('I020', 'O020', 'P002', 1, 6999.00),
('I021', 'O021', 'P008', 1, 9999.00),
('I022', 'O022', 'P004', 1, 3999.00);

-- 验证数据
SELECT '日期维度' as table_name, COUNT(*) as count FROM dim_date
UNION ALL SELECT '地区维度', COUNT(*) FROM dim_region
UNION ALL SELECT '商品类目', COUNT(*) FROM dim_category
UNION ALL SELECT '商品维度', COUNT(*) FROM dim_product
UNION ALL SELECT '订单事实', COUNT(*) FROM fact_order
UNION ALL SELECT '订单明细', COUNT(*) FROM fact_order_item;
