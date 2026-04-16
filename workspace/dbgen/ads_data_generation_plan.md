# ADS 层数据生成计划

> 本文档描述 `workspace/dbgen/etl_ads.py` 的数据生成策略。
> ADS 层是**应用层指标表**，面向具体业务场景（日报、ROI、留存、LTV 等），从 DWS/ODS 做最终聚合和指标封装。
> **核心原则**：能用 Doris SQL 一次性全量生成的，绝不使用 Python；仅在需要 Cohort / 留存矩阵等复杂结构化计算时才使用 Python 内存处理。

---

## 1. `ads_sales_kpi` — 销售 KPI 指标表

**数据量**：约 5,000 条（731 天 × ~7 个指标）

**生成方式**：`INSERT INTO ... SELECT` 一次性 SQL，从 `dws_sales_daily` 做 UNPIVOT 式插入，并计算环比/同比。

指标列表：`GMV`、`订单数`、`用户数`、`客单价`、`转化率`、`实付金额`、`利润`。

- `客单价` = `SUM(actual_amount) / COUNT(DISTINCT order_id)`
- `转化率`：模拟为 `paying_users / active_users * 100`，从 `dws_user_stats` 关联取数
- 环比/同比：使用 Doris `LAG()` 窗口函数或 Python 后处理；**测试数据阶段简化处理为 0**

```sql
INSERT INTO ads_sales_kpi
SELECT
  stat_date,
  'GMV' AS kpi_name,
  SUM(gmv) AS kpi_value,
  '元' AS kpi_unit,
  0.00 AS mom_growth,
  0.00 AS yoy_growth
FROM dws_sales_daily
GROUP BY stat_date
UNION ALL
SELECT stat_date, '实付金额', SUM(actual_amount), '元', 0.00, 0.00 FROM dws_sales_daily GROUP BY stat_date
UNION ALL
SELECT stat_date, '利润', SUM(profit_amount), '元', 0.00, 0.00 FROM dws_sales_daily GROUP BY stat_date
UNION ALL
SELECT stat_date, '订单数', SUM(order_count), '单', 0.00, 0.00 FROM dws_sales_daily GROUP BY stat_date
UNION ALL
SELECT stat_date, '用户数', SUM(order_user_count), '人', 0.00, 0.00 FROM dws_sales_daily GROUP BY stat_date
UNION ALL
SELECT
  a.stat_date,
  '客单价' AS kpi_name,
  ROUND(SUM(a.actual_amount) / NULLIF(SUM(a.order_count), 0), 2),
  '元',
  0.00, 0.00
FROM dws_sales_daily a
GROUP BY a.stat_date
```

转化率指标通过 `dws_user_stats` 关联生成单独批次插入。

---

## 2. `ads_user_retention` — 用户留存分析表

**数据量**：约 15 万条（每日注册 cohort × 4 个留存节点）

**生成方式**：Python 内存处理，**不按天循环 700 多次执行 SQL**。

逻辑：
1. 一次性读取 `ods_users`（`user_id`, `DATE(register_time)`）到内存
2. 一次性读取 `dwd_user_login`（`user_id`, `login_date`）到内存
3. 按 `register_date` 分组统计每日新增用户数
4. 对每一个 `register_date` 和 `retention_day ∈ {1, 3, 7, 30}`：
   - 计算 `target_date = register_date + retention_day`
   - 统计该 cohort 中在 `target_date` 有登录记录的去重用户数
   - 计算留存率 = 留存用户数 / 新增用户数
5. 一次性 `batch_insert` 写入

**为何不用纯 SQL**：Doris 中做 cohort 留存矩阵需要对每个留存天数做 `LEFT JOIN` 或 `LATERAL` 查询，SQL 冗长且难维护；Python 字典查找更直观高效。

---

## 3. `ads_region_rank` — 地区销售排行表

**数据量**：约 800 条（24 个月 × 34 省份）

**生成方式**：`INSERT INTO ... SELECT` 一次性 SQL，从 `dws_sales_monthly` 聚合并计算排名和份额。

```sql
WITH monthly AS (
  SELECT
    CAST(CONCAT(stat_month, '01') AS DATE) AS stat_date,
    province_id,
    SUM(gmv) AS gmv,
    SUM(order_count) AS order_count,
    SUM(order_user_count) AS user_count
  FROM dws_sales_monthly
  GROUP BY stat_month, province_id
),
total AS (
  SELECT stat_date, SUM(gmv) AS total_gmv FROM monthly GROUP BY stat_date
)
INSERT INTO ads_region_rank
SELECT
  m.stat_date,
  m.province_id,
  r.region_name AS province_name,
  m.gmv,
  m.order_count,
  m.user_count,
  ROW_NUMBER() OVER (PARTITION BY m.stat_date ORDER BY m.gmv DESC) AS gmv_rank,
  ROUND(m.gmv / NULLIF(t.total_gmv, 0), 4) AS gmv_share
FROM monthly m
JOIN dim_region r ON r.region_id = m.province_id
JOIN total t ON t.stat_date = m.stat_date
```

---

## 4. `ads_category_rank` — 类目销售排行表

**数据量**：约 700 条（24 个月 × ~30 类目）

**生成方式**：`INSERT INTO ... SELECT` 一次性 SQL，逻辑与 `ads_region_rank` 类似。

```sql
WITH monthly AS (
  SELECT
    CAST(CONCAT(stat_month, '01') AS DATE) AS stat_date,
    category_id,
    SUM(gmv) AS gmv,
    SUM(order_count) AS order_count,
    COUNT(DISTINCT product_id) AS product_count
  FROM dws_sales_monthly
  GROUP BY stat_month, category_id
),
total AS (
  SELECT stat_date, SUM(gmv) AS total_gmv FROM monthly GROUP BY stat_date
)
INSERT INTO ads_category_rank
SELECT
  m.stat_date,
  m.category_id,
  p.category_name,
  m.gmv,
  m.order_count,
  m.product_count,
  ROW_NUMBER() OVER (PARTITION BY m.stat_date ORDER BY m.gmv DESC) AS gmv_rank,
  ROUND(m.gmv / NULLIF(t.total_gmv, 0), 4) AS gmv_share
FROM monthly m
JOIN dim_product p ON p.category_id = m.category_id
JOIN total t ON t.stat_date = m.stat_date
```

---

## 5. `ads_member_lifecycle` — 会员生命周期指标表

**数据量**：约 240 条（24 个月 × ~10 个分层）

**生成方式**：`INSERT INTO ... SELECT` 一次性 SQL，从 `dws_member_daily` 按月聚合。

```sql
SELECT
  CAST(CONCAT(DATE_FORMAT(stat_date, '%Y%m'), '01') AS DATE) AS stat_date,
  seg.segment_name,
  SUM(md.new_members + md.active_members) AS member_count,
  ROUND(SUM(md.active_members) / NULLIF(SUM(md.new_members + md.active_members), 0), 4) AS active_rate,
  ROUND(SUM(md.renew_members) / NULLIF(SUM(md.new_members + md.active_members), 0), 4) AS renewal_rate,
  ROUND(SUM(md.member_gmv) / NULLIF(SUM(md.new_members + md.active_members), 0), 2) AS arpu
FROM dws_member_daily md
JOIN dim_user_segment seg ON seg.segment_id = md.segment_id
GROUP BY DATE_FORMAT(stat_date, '%Y%m'), seg.segment_name
```

---

## 6. `ads_marketing_roi` — 营销 ROI 指标表

**数据量**：约 6,000 条（730 天 × 8 渠道）

**生成方式**：`INSERT INTO ... SELECT` 一次性 SQL，从 `dwd_marketing_event` 聚合。

```sql
SELECT
  event_date AS stat_date,
  c.channel_name,
  SUM(cost_amount) AS cost_amount,
  SUM(cost_amount * 3.5) AS conversion_gmv,
  ROUND(SUM(cost_amount * 3.5) / NULLIF(SUM(cost_amount), 0), 4) AS roi,
  ROUND(SUM(cost_amount) / NULLIF(SUM(conversions), 0), 2) AS cac,
  ROUND(SUM(clicks) / NULLIF(SUM(impressions), 0), 4) AS ctr
FROM dwd_marketing_event e
JOIN dim_marketing_channel c ON c.channel_id = e.channel_id
GROUP BY event_date, c.channel_name
```

---

## 7. `ads_inventory_turnover` — 库存周转分析表

**数据量**：约 5,000 ~ 10,000 条（24 个月 × 活跃 SKU-仓库组合）

**生成方式**：`INSERT INTO ... SELECT` 一次性 SQL，从 `dws_inventory_daily` 聚合。

```sql
SELECT
  CAST(DATE_FORMAT(stat_date, '%Y%m') AS INT) AS stat_month,
  warehouse_id,
  product_id,
  AVG(closing_qty) AS avg_inventory,
  ROUND(30.0 * AVG(closing_qty) / NULLIF(SUM(outbound_qty) / COUNT(*), 0), 2) AS turnover_days,
  ROUND(NULLIF(SUM(outbound_qty), 0) / NULLIF(AVG(closing_qty), 0), 4) AS turnover_rate,
  SUM(CASE WHEN closing_qty = 0 THEN 1 ELSE 0 END) AS stockout_days
FROM dws_inventory_daily
GROUP BY stat_month, warehouse_id, product_id
```

---

## 8. `ads_stock_alert` — 库存预警表

**数据量**：约 2,000 ~ 5,000 条

**生成方式**：`INSERT INTO ... SELECT` 一次性 SQL，从 `ods_inventory` 直接生成当日预警快照。

```sql
SELECT
  '2026-04-16' AS alert_date,
  warehouse_id,
  product_id,
  CASE
    WHEN available_qty < 200 THEN 'low_stock'
    WHEN quantity > 4000 THEN 'over_stock'
    ELSE 'low_stock'
  END AS alert_type,
  quantity AS current_qty,
  200 AS safety_stock,
  GREATEST(0, 500 - available_qty) AS suggest_replenish_qty
FROM ods_inventory
WHERE available_qty < 200 OR quantity > 4000
```

---

## 9. `ads_mrr_kpi` — MRR KPI 指标表

**数据量**：约 3,600 条（731 天 × ~5 个指标）

**生成方式**：`INSERT INTO ... SELECT` 一次性 SQL，UNPIVOT 式插入。

```sql
INSERT INTO ads_mrr_kpi
SELECT
  stat_date,
  'MRR' AS kpi_name,
  total_mrr AS kpi_value,
  0.00 AS mom_growth,
  '元' AS unit
FROM dws_mrr_daily
UNION ALL
SELECT
  stat_date,
  'ARR',
  total_mrr * 12,
  0.00,
  '元'
FROM dws_mrr_daily
UNION ALL
SELECT
  d.stat_date,
  'ChurnRate',
  ROUND(ABS(d.churn_mrr) / NULLIF(d.total_mrr + ABS(d.churn_mrr), 0), 4) * 100,
  0.00,
  '%'
FROM dws_mrr_daily d
```

---

## 10. `ads_ltv_cohort` — LTV Cohort 分析表

**数据量**：约 5,000 ~ 10,000 条

**生成方式**：Python 内存处理。

逻辑：
1. 读取 `ods_subscriptions`（`customer_id`, `start_date`, `channel_id`）到内存
2. 读取 `dwd_revenue_detail`（`customer_id`, `revenue_date`, `recognized_amount`）到内存
3. 按 `cohort_month = DATE_FORMAT(start_date, '%Y%m')` 分组
4. 对每个 cohort，按 `period_month = DATE_FORMAT(revenue_date, '%Y%m')` 汇总：
   - `customer_count`：cohort 中在 `period_month` 仍有收入的客户数
   - `revenue`：该 cohort 在 `period_month` 的确认收入
   - `ltv`：cohort 到该月为止的累计收入 / 该 cohort 总人数
   - `retention_rate`：`period_month` 的存活客户数 / cohort 总人数
5. 一次性 `batch_insert`

**为何不用纯 SQL**：LTV Cohort 需要多层自关联和累计窗口，SQL 可读性差；Python `defaultdict` + `groupby` 更清晰。

---

## 11. `ads_nps_score` — NPS 评分表

**数据量**：约 48 条（24 个月 × 问卷批次）

**生成方式**：`INSERT INTO ... SELECT` 一次性 SQL，从 `ods_tickets` 的 `csat_score` 映射为 NPS 三分类。

NPS 映射规则（测试数据简化）：
- `csat_score = 5` → 推荐者 (promoter)
- `csat_score = 4` → 被动者 (passive)
- `csat_score ≤ 3` → 贬损者 (detractor)

```sql
SELECT
  CAST(DATE_FORMAT(create_date, '%Y%m') AS INT) AS stat_month,
  SUM(CASE WHEN csat_score = 5 THEN 1 ELSE 0 END) AS promoters,
  SUM(CASE WHEN csat_score = 4 THEN 1 ELSE 0 END) AS passives,
  SUM(CASE WHEN csat_score <= 3 THEN 1 ELSE 0 END) AS detractors,
  ROUND(
    (SUM(CASE WHEN csat_score = 5 THEN 1 ELSE 0 END) - SUM(CASE WHEN csat_score <= 3 THEN 1 ELSE 0 END))
    * 100.0 / COUNT(*),
    2
  ) AS nps_score,
  COUNT(*) AS response_count
FROM ods_tickets
WHERE csat_score IS NOT NULL
GROUP BY DATE_FORMAT(create_date, '%Y%m')
```

---

## 12. `ads_saas_user_segment` — SaaS 用户分群表

**数据量**：约 6,000 条（730 天 × ~8 个分群）

**生成方式**：`INSERT INTO ... SELECT` 一次性 SQL，从 `ods_subscriptions` 和 `dwd_revenue_detail` 聚合。

分群规则（测试数据简化）：
- `VIP`：套餐 ID = 5（定制版）
- `高活跃`：套餐 ID = 4（企业版）
- `中活跃`：套餐 ID = 3（专业版）
- `低活跃`：套餐 ID = 2（基础版）
- `免费用户`：套餐 ID = 1（免费版）
- `流失风险`：status = 2（取消）

```sql
WITH daily AS (
  SELECT
    s.start_date AS stat_date,
    CASE
      WHEN s.plan_id = 5 THEN 'VIP'
      WHEN s.plan_id = 4 THEN '高活跃'
      WHEN s.plan_id = 3 THEN '中活跃'
      WHEN s.plan_id = 2 THEN '低活跃'
      WHEN s.plan_id = 1 THEN '免费用户'
    END AS segment_name,
    COUNT(*) AS customer_count,
    SUM(s.mrr) AS mrr_contribution,
    85.00 AS avg_health_score
  FROM ods_subscriptions s
  WHERE s.status = 1
  GROUP BY s.start_date, s.plan_id
)
INSERT INTO ads_saas_user_segment
SELECT * FROM daily
UNION ALL
SELECT
  DATE(e.event_date) AS stat_date,
  '流失风险' AS segment_name,
  COUNT(*) AS customer_count,
  SUM(ABS(e.mrr_change)) AS mrr_contribution,
  30.00 AS avg_health_score
FROM dwd_subscription_events e
WHERE e.event_type = 'churn'
GROUP BY DATE(e.event_date)
```

---

## 执行顺序

ADS 表之间无依赖，按域分组执行更清晰：

1. `ads_sales_kpi`
2. `ads_region_rank`
3. `ads_category_rank`
4. `ads_marketing_roi`
5. `ads_member_lifecycle`
6. `ads_inventory_turnover`
7. `ads_stock_alert`
8. `ads_mrr_kpi`
9. `ads_saas_user_segment`
10. `ads_nps_score`
11. `ads_user_retention`（Python）
12. `ads_ltv_cohort`（Python）
