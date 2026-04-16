# DWD 层数据生成计划

> 本文档描述 `workspace/dbgen/etl_dim_dwd.py` 中 DWD 事实表的数据生成策略。
> 严格遵循**维度建模**理论：DWD 层以 ODS 原始数据为基础，通过关联维度表做**轻度清洗、退维、计算衍生指标**，形成一致的事实明细。
> 所有数据生成均通过 **一次性全量处理** 完成，**不按天循环 700 多次**执行 SQL。

---

## 核心原则

1. **退化维度**：将频繁使用的维度属性（如订单状态、支付方式、省份城市）直接冗余到事实表，减少 JOIN 开销。
2. **一次性全量**：所有 `INSERT` 均基于 ODS 全量数据 `SELECT`，不加日期过滤条件；需要拆行/模拟的表在 Python 内存中全量生成后 batch insert。
3. **可追踪**：所有 DWD 数据均可从 ODS 数据推导得到，保证数据血缘清晰。

---

## 1. `dwd_order_detail` — 订单明细宽表

**数据量**：约 64.5 万条（与 `ods_order_items` 同粒度）

**生成方式**：`INSERT INTO ... SELECT` 一次性 SQL，关联 `ods_order_items`、`ods_orders`、`ods_users`、`ods_products`。

| 字段 | 填充策略 |
|------|----------|
| `order_id` / `order_item_id` | 直接取 `ods_order_items` |
| `user_id` | 取 `ods_orders.user_id` |
| `product_id` / `category_id` / `brand_id` | 取 `ods_order_items.product_id`，`category_id` 和 `brand_id` 通过 `ods_products` JOIN 补全 |
| `quantity` / `unit_price` | 取 `ods_order_items` |
| `discount_amount` | 按商品行金额占订单总金额比例**分摊**订单级优惠：`ods_orders.discount_amount * (item.total_amount / order.total_amount)` |
| `pay_amount` | `unit_price * quantity - discount_amount`（行级实付） |
| `unit_cost` | 取 `ods_products.cost` |
| `profit` | `(unit_price - unit_cost) * quantity` |
| `order_status` / `pay_type` | 退化维度，取 `ods_orders` |
| `province_id` / `city_id` | 退化维度，取 `ods_users` |
| `order_date` | `DATE(ods_orders.create_time)` |
| `order_month` | `YYYYMM` 格式 |
| `pay_time` / `create_time` | 取 `ods_orders` 对应字段 |

---

## 2. `dwd_user_login` — 用户登录事实表

**数据量**：约 3 万条（`ods_app_events` 中 `event_name='login'` 的子集）

**生成方式**：`INSERT INTO ... SELECT` 一次性 SQL，从 `ods_app_events` 过滤 login 事件，JOIN `ods_users` 补全地理和渠道信息。

| 字段 | 填充策略 |
|------|----------|
| `login_id` | 取 `ods_app_events.event_id` |
| `user_id` | 取 `ods_app_events.user_id` |
| `login_date` | `DATE(ods_app_events.event_time)` |
| `login_time` | `ods_app_events.event_time` |
| `device_type` | 映射 `ods_app_events.device_type`（`ios`→`iOS`, `android`→`Android`, `web`→`PC/H5`, `miniapp`→`H5`） |
| `province_id` | 通过 `JOIN ods_users` 获取 |
| `channel_id` | 随机分配 `1~8`（模拟引流渠道），通过 Doris `rand()` 函数或 Python 后处理完成 |

---

## 3. `dwd_marketing_event` — 营销事件事实表

**数据量**：约 4,000 ~ 6,000 条

**生成方式**：Python 内存中生成模拟数据，一次性 batch insert。

**原因**：ODS 层没有直接对应的营销事件原始表，DWD 层需要基于业务假设生成。

| 字段 | 填充策略 |
|------|----------|
| `event_id` | 自增 BIGINT |
| `event_date` | 2024-04-16 ~ 2026-04-16 随机分布 |
| `channel_id` | 从 `dim_marketing_channel` 中随机采样（`1~8`） |
| `event_type` | `ad`(45%), `promotion`(35%), `coupon`(20%) |
| `cost_amount` | 500 ~ 50,000 元随机 |
| `impressions` | `cost_amount * (50~200)` |
| `clicks` | `impressions * (0.01~0.05)` |
| `conversions` | `clicks * (0.02~0.10)` |

---

## 4. `dwd_inventory_movement` — 库存流水事实表

**数据量**：约 15 万 ~ 20 万条

**生成方式**：Python 读取 ODS 数据后在内存中生成，一次性 batch insert。

**来源拆解**：
1. **采购入库**：将 `ods_purchase_orders` 每条拆分为 `1~3` 条 `inbound` 记录，从 `ods_products` 随机选取 `product_id`，数量按采购总金额反推。
2. **销售出库**：将 `ods_order_items` 直接映射为 `outbound` 记录，`quantity` 取负值。
3. **盘点调整**：基于 `ods_inventory` 生成约 5,000 条 `adjust`/`loss` 记录。

| 字段 | 填充策略 |
|------|----------|
| `movement_id` | 自增 BIGINT |
| `movement_date` | 采购单取 `po_date`，销售出库取订单 `create_time` 日期，调整取随机近期日期 |
| `warehouse_id` | 采购取 `ods_purchase_orders.warehouse_id`，销售取随机仓库（因为 ods_orders 无仓库） |
| `product_id` | 采购随机选，销售直接取 `ods_order_items.product_id` |
| `movement_type` | `inbound` / `outbound` / `adjust` / `loss` |
| `quantity` | 入库为正，出库为负，调整为 ± |
| `related_order_id` | 关联采购单 ID 或订单 ID |

---

## 5. `dwd_purchase_detail` — 采购明细事实表

**数据量**：约 1.5 万条（将 5,500 条采购单拆分为 SKU 行）

**生成方式**：Python 读取 `ods_purchase_orders` 后在内存中拆行，一次性 batch insert。

| 字段 | 填充策略 |
|------|----------|
| `po_id` / `supplier_id` / `warehouse_id` | 直接映射 `ods_purchase_orders` |
| `po_item_id` | 自增 BIGINT |
| `product_id` | 从 `ods_products` 中随机采样（status=1 优先） |
| `quantity` | 100 ~ 5,000 随机 |
| `unit_price` | 取 `ods_products.cost * (0.8~1.2)` |
| `total_amount` | `quantity * unit_price` |

**分摊逻辑**：所有 item 的 `total_amount` 之和约等于 `ods_purchase_orders.total_amount`（允许 ±5% 误差）。

---

## 6. `dwd_subscription_events` — 订阅事件事实表

**数据量**：约 30 万条（基于 2.8 万 subscriptions + 25.7 万 payments）

**生成方式**：Python 读取 `ods_subscriptions` 和 `ods_payments`，在内存中生成事件后一次性 batch insert。

| 事件类型 | 触发条件 |
|----------|----------|
| `new` | 每个 `subscription.start_date` 产生一条 |
| `renewal` | 每个成功的 `ods_payments`（非退款）产生一条，事件日期 = `payment_date` |
| `churn` | 每个 `subscription.status=2（取消）` 在 `end_date` 产生一条 |
| `upgrade` / `downgrade` | 随机挑选 5% 的 subscription，在其生命周期中间生成一条套餐变更事件 |

| 字段 | 填充策略 |
|------|----------|
| `event_id` | 自增 BIGINT |
| `event_date` | 见上表触发条件 |
| `customer_id` / `subscription_id` | 直接映射 |
| `event_type` | `new` / `renewal` / `upgrade` / `downgrade` / `churn` |
| `plan_id` | 当前/变更后套餐 ID |
| `mrr_change` | `new/renewal` 为 `+mrr`；`churn` 为 `-mrr`；`upgrade/downgrade` 为差额 |
| `previous_plan_id` | `upgrade/downgrade` 填原套餐，其他事件可填自身 plan_id 或 NULL |

---

## 7. `dwd_revenue_detail` — 收入明细事实表

**数据量**：约 80 万条

**生成方式**：Python 读取 `ods_payments`，在内存中按**收入确认周期**拆分，一次性 batch insert。

**拆分规则**：
- **月付**（`amount ≈ mrr`）：1 条记录，`revenue_date = payment_date`，`recognized_amount = gross_amount`
- **年付**（`amount > mrr * 5`）：拆分为 12 条月度确认记录，从 `payment_date` 开始每月一条，每月 `recognized_amount = gross_amount / 12`
- **退款**（`amount < 0`）：1 条记录，`recognized_amount = amount`（负数冲减）

| 字段 | 填充策略 |
|------|----------|
| `revenue_id` | 自增 BIGINT |
| `payment_id` / `customer_id` / `subscription_id` | 直接映射 `ods_payments` |
| `revenue_date` | 收入归属日期（月付=付款日，年付=各月首日） |
| `gross_amount` | 原始付款金额 |
| `recognized_amount` | 当期确认收入 |
| `plan_id` | 通过 `ods_payments.subscription_id` 关联 `ods_subscriptions` 获取 |

---

## 执行顺序

DWD 表之间存在数据血缘，但互相不依赖，因此可以按任意顺序执行。推荐顺序：

1. `dwd_order_detail`（电商核心事实）
2. `dwd_user_login`（基于 app events）
3. `dwd_marketing_event`（模拟生成）
4. `dwd_purchase_detail`（供应链采购明细）
5. `dwd_inventory_movement`（基于采购明细 + 订单明细）
6. `dwd_subscription_events`（SaaS 订阅事件）
7. `dwd_revenue_detail`（SaaS 收入确认）

---

## 技术实现要点

- **纯 SQL 生成的表**：直接通过 `cursor.execute(sql)` 执行 `TRUNCATE` + `INSERT INTO ... SELECT`。
- **Python 拆行/模拟生成的表**：先用 `cursor.execute("SELECT ...")` 读取 ODS 全量数据到内存，在 Python 中构造目标行列表，最后调用 `cursor.executemany()` **一次性 batch insert**。
- **无按天循环**：即使对于日期相关计算，也只在 Python 内存中做日期运算，不会按天向 Doris 发起 700+ 次 SQL 请求。
