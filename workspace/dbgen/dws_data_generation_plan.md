# DWS 层数据生成计划

> 本文档描述 `workspace/dbgen/etl_dws.py` 的数据生成策略。
> 严格遵循维度建模理论，DWS 层基于 DWD 事实表做**维度退化聚合**，形成日/月级汇总指标。
> **核心原则**：尽量使用 Doris SQL 一次性全量执行；仅在单表内无法表达的复杂逻辑（如库存日快照的期初期末推导）才使用 Python 内存处理。

---

## 1. `dws_sales_daily` — 日销售汇总表

**数据量**：约 5 ~ 10 万条

**生成方式**：`INSERT INTO ... SELECT` 一次性 SQL，从 `dwd_order_detail` 聚合。

```sql
SELECT
  order_date AS stat_date,
  province_id,
  category_id,
  COUNT(DISTINCT order_id) AS order_count,
  COUNT(DISTINCT user_id) AS order_user_count,
  COUNT(DISTINCT product_id) AS product_count,
  SUM(unit_price * quantity) AS gmv,
  SUM(discount_amount) AS discount_amount,
  SUM(pay_amount) AS actual_amount,
  SUM(unit_cost * quantity) AS cost_amount,
  SUM(profit) AS profit_amount
FROM dwd_order_detail
GROUP BY order_date, province_id, category_id
```

---

## 2. `dws_sales_monthly` — 月销售汇总表

**数据量**：约 3 ~ 5 万条

**生成方式**：`INSERT INTO ... SELECT` 一次性 SQL，从 `dwd_order_detail` 按月聚合。

```sql
SELECT
  order_month AS stat_month,
  province_id,
  category_id,
  COUNT(DISTINCT order_id) AS order_count,
  COUNT(DISTINCT user_id) AS order_user_count,
  COUNT(DISTINCT product_id) AS product_count,
  SUM(unit_price * quantity) AS gmv,
  SUM(discount_amount) AS discount_amount,
  SUM(pay_amount) AS actual_amount,
  SUM(unit_cost * quantity) AS cost_amount,
  SUM(profit) AS profit_amount
FROM dwd_order_detail
GROUP BY order_month, province_id, category_id
```

---

## 3. `dws_user_stats` — 用户统计表

**数据量**：731 条（每天一行）

**生成方式**：`INSERT INTO ... SELECT` 一次性 SQL，多源聚合。

```sql
WITH new_users AS (
  SELECT DATE(register_time) AS d, COUNT(*) AS c FROM ods_users GROUP BY d
),
active_users AS (
  SELECT login_date AS d, COUNT(DISTINCT user_id) AS c FROM dwd_user_login GROUP BY login_date
),
order_stats AS (
  SELECT
    order_date AS d,
    COUNT(DISTINCT order_id) AS order_count,
    COUNT(DISTINCT user_id) AS paying_users,
    SUM(pay_amount) AS gmv
  FROM dwd_order_detail
  GROUP BY order_date
),
accum AS (
  SELECT DATE(register_time) AS d, COUNT(*) AS total_users FROM ods_users GROUP BY d
)
SELECT
  dd.date_str AS stat_date,
  COALESCE(nu.c, 0) AS new_users,
  COALESCE(au.c, 0) AS active_users,
  COALESCE(os.paying_users, 0) AS paying_users,
  ROUND(COALESCE(os.gmv, 0) / NULLIF(os.paying_users, 0), 2) AS gmv_per_user,
  ROUND(COALESCE(os.order_count, 0) / NULLIF(os.paying_users, 0), 2) AS orders_per_user,
  SUM(COALESCE(ac.total_users, 0)) OVER (ORDER BY dd.date_str) AS total_users
FROM dim_date dd
LEFT JOIN new_users nu ON nu.d = dd.date_str
LEFT JOIN active_users au ON au.d = dd.date_str
LEFT JOIN order_stats os ON os.d = dd.date_str
LEFT JOIN accum ac ON ac.d = dd.date_str
```

---

## 4. `dws_member_daily` — 会员日汇总表

**数据量**：约 5,000 ~ 8,000 条（730 天 × ~10 个分层）

**生成方式**：`INSERT INTO ... SELECT` 一次性 SQL，基于 `dwd_order_detail` 和 `ods_users` 做模拟分层聚合。

分层逻辑（测试数据简化版）：
- `segment_id=1 新客`：注册 < 30 天且有下单
- `segment_id=2 活跃用户`：当日有登录或下单
- `segment_id=3 沉睡用户`：30~90 天未活跃
- `segment_id=5 高价值用户`：累计消费 TOP20%
- `segment_id=7 低价值用户`：累计消费 BOTTOM40%

使用 SQL `CASE WHEN` 将用户打上标签后按日期和 `segment_id` 聚合。其余分层（RFM 类）数据量设为 0。

---

## 5. `dws_marketing_daily` — 营销日汇总表

**数据量**：约 5,000 ~ 6,000 条

**生成方式**：`INSERT INTO ... SELECT` 一次性 SQL，从 `dwd_marketing_event` 聚合。

```sql
SELECT
  event_date AS stat_date,
  channel_id,
  SUM(impressions) AS impressions,
  SUM(clicks) AS clicks,
  SUM(conversions) AS conversions,
  SUM(cost_amount) AS cost_amount,
  SUM(cost_amount * 3.5) AS conversion_gmv   -- 模拟转化GMV = cost * 3.5
FROM dwd_marketing_event
GROUP BY event_date, channel_id
```

---

## 6. `dws_inventory_daily` — 库存日汇总表

**数据量**：约 50 万条（月维度快照，仅保留每月最后一天）

**生成方式**：Python 内存处理，**不按天循环 700 多次执行 SQL**。

处理逻辑：
1. 一次性 `SELECT` 读取 `dwd_inventory_movement` 全量数据到内存
2. 按 `(warehouse_id, product_id, movement_date)` 汇总每日净变动
3. 对每个 SKU-仓库组合，按日期排序并计算**累计库存**（running sum）
4. 只保留**每月最后一天**的快照记录，生成 `opening_qty`、`inbound_qty`、`outbound_qty`、`closing_qty`
5. `available_qty` = `closing_qty * 0.85`（模拟可用比例）
6. 一次性 `batch_insert` 写入 Doris

**为何不用纯 SQL**：Doris 中做每日 SKU-仓库级期初期末快照需要 `CROSS JOIN` 日期维度和库存组合，会产生数千万行中间结果，在测试数据场景下性价比极低。Python 内存计算更可控。

---

## 7. `dws_purchase_monthly` — 采购月汇总表

**数据量**：约 3,000 ~ 8,000 条

**生成方式**：`INSERT INTO ... SELECT` 一次性 SQL，从 `dwd_purchase_detail` 按月聚合。

```sql
SELECT
  CAST(DATE_FORMAT(po_date, '%Y%m') AS INT) AS stat_month,
  supplier_id,
  p.category_id,
  COUNT(DISTINCT pd.po_id) AS po_count,
  SUM(pd.quantity) AS po_quantity,
  SUM(pd.total_amount) AS po_amount
FROM dwd_purchase_detail pd
JOIN ods_purchase_orders op ON op.po_id = pd.po_id
JOIN ods_products p ON p.product_id = pd.product_id
GROUP BY stat_month, supplier_id, p.category_id
```

---

## 8. `dws_mrr_daily` — MRR 日汇总表

**数据量**：731 条

**生成方式**：`INSERT INTO ... SELECT` 一次性 SQL，从 `dwd_subscription_events` 聚合。

```sql
SELECT
  event_date AS stat_date,
  SUM(CASE WHEN event_type = 'new' THEN mrr_change ELSE 0 END) AS new_mrr,
  SUM(CASE WHEN event_type = 'churn' THEN -mrr_change ELSE 0 END) AS churn_mrr,
  SUM(CASE WHEN event_type = 'upgrade' THEN mrr_change ELSE 0 END) AS expansion_mrr,
  SUM(CASE WHEN event_type = 'downgrade' THEN -mrr_change ELSE 0 END) AS contraction_mrr,
  SUM(CASE WHEN event_type IN ('new', 'renewal', 'upgrade') THEN mrr_change ELSE 0 END)
    - SUM(CASE WHEN event_type IN ('churn', 'downgrade') THEN -mrr_change ELSE 0 END) AS total_mrr
FROM dwd_subscription_events
GROUP BY event_date
```

---

## 9. `dws_churn_monthly` — 流失月汇总表

**数据量**：约 200 ~ 500 条（24 个月 × ~20 个套餐组合）

**生成方式**：`INSERT INTO ... SELECT` 一次性 SQL。

```sql
WITH churned AS (
  SELECT
    CAST(DATE_FORMAT(event_date, '%Y%m') AS INT) AS stat_month,
    plan_id,
    COUNT(DISTINCT customer_id) AS churned_customers,
    SUM(mrr_change) AS churn_mrr,
    AVG(DATEDIFF(event_date, start_date)) AS avg_tenure_days
  FROM dwd_subscription_events e
  JOIN ods_subscriptions s ON s.subscription_id = e.subscription_id
  WHERE event_type = 'churn'
  GROUP BY stat_month, plan_id
),
total AS (
  SELECT plan_id, COUNT(DISTINCT customer_id) AS total_customers
  FROM ods_subscriptions
  GROUP BY plan_id
)
SELECT
  c.stat_month,
  c.plan_id,
  c.churned_customers,
  ROUND(c.churned_customers / NULLIF(t.total_customers, 0), 4) AS churn_rate,
  c.churn_mrr,
  CAST(c.avg_tenure_days AS INT) AS avg_tenure_days
FROM churned c
JOIN total t ON t.plan_id = c.plan_id
```

---

## 10. `dws_user_activity_daily` — 用户活动日汇总表

**数据量**：731 条

**生成方式**：`INSERT INTO ... SELECT` 一次性 SQL，从 `ods_app_events` 和 `ods_users` 聚合。

```sql
SELECT
  e.event_date AS stat_date,
  COUNT(DISTINCT e.user_id) AS dau,
  COUNT(CASE WHEN e.event_name = 'feature_use' THEN 1 END) AS feature_use_count,
  ROUND(RAND() * 30 + 10, 2) AS avg_session_minutes,  -- 模拟数据
  COUNT(DISTINCT CASE WHEN u.register_time >= e.event_date AND u.register_time < DATE_ADD(e.event_date, 1) THEN e.user_id END) AS new_user_count,
  COUNT(DISTINCT CASE WHEN u.register_time < e.event_date THEN e.user_id END) AS returning_user_count
FROM ods_app_events e
JOIN ods_users u ON u.user_id = e.user_id
GROUP BY e.event_date
```

---

## 执行顺序

DWS 表之间无强依赖，推荐顺序：
1. `dws_sales_daily`
2. `dws_sales_monthly`
3. `dws_user_stats`
4. `dws_marketing_daily`
5. `dws_member_daily`
6. `dws_purchase_monthly`
7. `dws_mrr_daily`
8. `dws_churn_monthly`
9. `dws_user_activity_daily`
10. `dws_inventory_daily`（Python 处理，放最后）
