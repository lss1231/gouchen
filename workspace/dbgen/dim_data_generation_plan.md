# DIM 层数据生成计划

> 本文档描述 `workspace/dbgen/etl_dim_dwd.py` 中 DIM 表的数据生成策略。
> 遵循维度建模理论，DIM 表为后续 DWD/DWS/ADS 层提供一致的维度视图。
> 所有 DIM 表数据均通过 **一次性全量插入** 完成，不按天循环。

---

## 1. `dim_date` — 日期维度表

**生成方式**：Python 代码生成 2024-04-16 ~ 2026-04-16 的完整日期序列（730 天），batch insert。

| 字段 | 填充策略 |
|------|----------|
| `date_key` | `YYYYMMDD` 格式，如 `20240416` |
| `date_str` | `YYYY-MM-DD` 格式 |
| `day_of_week` | `1=周一` 到 `7=周日` |
| `day_name` | 对应中文星期名称，如 `星期一` |
| `month_name` | 对应中文月份名称，如 `四月` |
| `year` / `month` / `day` | 公历年月日数字 |
| `quarter` | 季度 `1~4` |
| `week_of_year` | 年内第几周 `1~53` |
| `is_weekend` | 周六/周日为 `true`，否则 `false` |
| `is_holiday` | 标记春节(2025-01-29~02-04)、国庆(10-01~10-07)、元旦、劳动节等法定节假日为 `true` |
| `holiday_name` | 节假日中文名称，非节假日为空字符串 |

---

## 2. `dim_region` — 地区维度表

**生成方式**：Python 直接插入中国 34 个省级行政区的基础信息（只做省级，匹配 ODS 中实际使用的 `province_id`）。

| 字段 | 填充策略 |
|------|----------|
| `region_id` | 与 `ods_users.province_id` 对齐，`1~34` |
| `region_name` | 省/直辖市/自治区名称，如 `北京市`、`广东省` |
| `region_level` | 固定为 `1`（省级） |
| `parent_id` | 固定为 `0` |
| `region_path` | `0/{region_id}` |
| `region_code` | 国标行政区划代码前两位补零，如 `110000`、`440000` |

**策略**：不做市/区三级下沉，因为当前分析场景以省级为主，且 ODS 中城市 ID 是 province_id 的派生值。

---

## 3. `dim_product` — 商品维度表

**生成方式**：`INSERT INTO dim_product SELECT ... FROM ods_products`，一次性 SQL 全量插入。

| 字段 | 填充策略 |
|------|----------|
| `product_id` / `product_name` | 直接映射 `ods_products` |
| `category_id` | 直接映射 |
| `category_name` | 用固定字典映射 `category_id` → 类目路径，如 `数码/手机/智能手机` |
| `brand_id` | 直接映射 |
| `brand_name` | 用固定字典映射 `brand_id` → 品牌中文名，如 `华为`、`小米` |
| `price` / `cost` | 直接映射 |
| `profit_rate` | SQL 计算：`(price - cost) / price`，保留 4 位小数 |
| `status` | 直接映射 |
| `create_date` / `update_date` | 取 `ods_products.create_time / update_time` 的日期部分 |

---

## 4. `dim_user_segment` — 用户分层维度表

**生成方式**：Python 直接插入固定配置数据（约 10 条）。

| 数据示例 |
|----------|
| `(1, '新客', 'lifecycle', '注册30天内的新用户')` |
| `(2, '活跃用户', 'lifecycle', '近30天有登录或下单的用户')` |
| `(3, '沉睡用户', 'lifecycle', '30~90天未活跃的用户')` |
| `(4, '流失用户', 'lifecycle', '超过90天未活跃的用户')` |
| `(5, '高价值用户', 'value', '累计消费TOP20%的用户')` |
| `(6, '中价值用户', 'value', '累计消费20%~60%的用户')` |
| `(7, '低价值用户', 'value', '累计消费BOTTOM40%的用户')` |
| `(8, 'RFM重要保持', 'rfm', 'RFM模型中的重要保持客户')` |
| `(9, 'RFM重要挽留', 'rfm', 'RFM模型中的重要挽留客户')` |
| `(10, 'RFM一般客户', 'rfm', 'RFM模型中的一般客户')` |

---

## 5. `dim_marketing_channel` — 营销渠道维度表

**生成方式**：Python 直接插入固定配置数据（8 条）。

| 数据示例 |
|----------|
| `(1, '抖音', 'social', 0)` |
| `(2, '微信朋友圈', 'social', 0)` |
| `(3, '百度搜索', 'paid', 0)` |
| `(4, '自然搜索', 'organic', 0)` |
| `(5, '信息流广告', 'paid', 0)` |
| `(6, '邮件营销', 'email', 0)` |
| `(7, 'KOL合作', 'social', 0)` |
| `(8, '线下活动', 'organic', 0)` |

---

## 6. `dim_warehouse` — 仓库维度表

**生成方式**：`INSERT INTO dim_warehouse SELECT ... FROM ods_warehouses`，一次性 SQL 全量插入，补充地理名称。

| 字段 | 填充策略 |
|------|----------|
| `warehouse_id` / `warehouse_name` / `warehouse_type` / `capacity` / `status` | 直接映射 `ods_warehouses` |
| `province_name` | 通过 `JOIN dim_region` 获取，或 Python 字典映射 `province_id` → 省份名称 |
| `city_name` | 简化为与省份名称相同（因 ODS 中城市是省份派生） |

---

## 7. `dim_supplier` — 供应商维度表

**生成方式**：`INSERT INTO dim_supplier SELECT ... FROM ods_suppliers` + `UPDATE` 补充计算字段。

| 字段 | 填充策略 |
|------|----------|
| `supplier_id` / `supplier_name` / `supplier_level` / `status` | 直接映射 |
| `category_id` | 随机分配 `1~30`（模拟主营类目） |
| `cooperation_years` | 随机 `1~5` 年 |
| `rating_score` | 随机 `3.00~5.00`，A 级供应商偏向高分 |

---

## 8. `dim_plan` — 套餐维度表

**生成方式**：Python 直接插入固定配置数据（5 条）。

| 数据示例 |
|----------|
| `(1, '免费版', 1, 0.00, 0.00, 'monthly', 1)` |
| `(2, '基础版', 1, 99.00, 999.00, 'monthly', 1)` |
| `(3, '专业版', 2, 299.00, 2999.00, 'monthly', 1)` |
| `(4, '企业版', 3, 999.00, 9999.00, 'annual', 1)` |
| `(5, '定制版', 3, 4999.00, 49999.00, 'annual', 1)` |

---

## 9. `dim_channel` — SaaS 渠道维度表

**生成方式**：Python 直接插入固定配置数据（8 条）。

| 数据示例 |
|----------|
| `(1, 'SEO', 'organic', 'fixed')` |
| `(2, 'SEM', 'paid', 'cpc')` |
| `(3, '内容营销', 'organic', 'fixed')` |
| `(4, '线下活动', 'organic', 'fixed')` |
| `(5, '客户推荐', 'referral', 'cpa')` |
| `(6, '代理商', 'partner', 'fixed')` |
| `(7, '社交媒体广告', 'paid', 'cpm')` |
| `(8, '产品内增长', 'organic', 'fixed')` |

---

## 执行顺序

1. 先执行需要 Python 直接 batch insert 的表：`dim_date`、`dim_region`、`dim_user_segment`、`dim_marketing_channel`、`dim_plan`、`dim_channel`
2. 再执行从 ODS 清洗的表：`dim_product`、`dim_warehouse`、`dim_supplier`
