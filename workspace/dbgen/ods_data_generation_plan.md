# ODS 层模拟数据生成计划

> 本文档描述 `workspace/dbgen/ods_generate_data.py` 脚本的数据生成策略与规模设计。
> 数据时间跨度：**2024-04-16 ~ 2026-04-16**（共 730 天）。

---

## 全局约束

| 指标 | 数值 |
|------|------|
| 总用户规模 | **10 万人** |
| 平均日活 (DAU) | **2 万人** |
| 平均日订单 | **1,000 ~ 2,000**（近期），早期更低 |
| 数据时间跨度 | **2024-04-16 ~ 2026-04-16**（730 天） |
| 订单总量上限 | **≤ 50 万** |
| `ods_app_events` | **10 万条**（人为封顶） |
| ETL 时间基准 | `2026-04-16 00:00:00` |

---

## 一、电商域 ODS

### 1. `ods_users` — ODS 用户源数据表

**数据量：10 万条**

| 字段 | 生成策略 |
|------|----------|
| `user_id` | 自增 BIGINT，1 ~ 100,000 |
| `username` | Faker 中文昵称，fallback 为 `user_{id}` |
| `email` / `phone` | Faker 随机，保证唯一 |
| `gender` | 0/1，比例约 45:55 |
| `birthday` | 18~50 岁随机，即 1976~2008 年之间 |
| `register_time` | **非均匀增长**：从日均 50 人增长至 500 人，整体按权重分配 10 万个注册时间点 |
| `last_login_time` | 80% 的用户在最近 30 天内有登录；其余在注册后随机分布 |
| `user_status` | 90% 正常(1)，10% 禁用(0) |
| `city_id` / `province_id` | 从 34 个省级行政区按人口权重随机抽取 |
| `etl_time` | 固定为 `2026-04-16 00:00:00` |

**生成方式**：按天分配注册配额，循环生成 10 万条用户。

---

### 2. `ods_products` — ODS 商品源数据表

**数据量：3,000 条**

| 字段 | 生成策略 |
|------|----------|
| `product_id` | 自增 BIGINT，1 ~ 3,000 |
| `product_name` | Faker 商品名或 `商品_{id}` |
| `category_id` | 1 ~ 30（类目） |
| `brand_id` | 1 ~ 100（品牌） |
| `price` | 正态分布，均值 150 元，标准差 80，最低 9.9 元 |
| `cost` | `price * (0.4 ~ 0.8)` |
| `stock` | 0 ~ 10,000 随机 |
| `status` | 85% 上架(1)，15% 下架(0) |
| `create_time` | 2024 年之前到近期均匀分布 |
| `update_time` | `create_time` ~ 2026-04-16 之间随机 |
| `etl_time` | 固定 |

**生成方式**：直接批量生成 3,000 条，无复杂依赖。

---

### 3. `ods_orders` — ODS 订单源数据表

**数据量：约 47.5 万条**

分段日均订单数设计：
- 早期（2024-04 ~ 2024-12）：约 250 单/天
- 中期（2025-01 ~ 2025-09）：约 600 单/天
- 近期（2025-10 ~ 2026-04）：约 1,300 单/天

| 字段 | 生成策略 |
|------|----------|
| `order_id` | 自增 BIGINT，从 1,000,001 开始 |
| `user_id` | 从 `ods_users` 中按注册时间**有偏采样**（下单用户必须已注册） |
| `order_status` | 1(待支付)5%，2(已支付)15%，3(已发货)20%，4(已完成)55%，5(已取消)5% |
| `total_amount` | 均值 180 元，正态分布 |
| `discount_amount` | `total * (0 ~ 0.25)` |
| `pay_amount` | `total - discount` |
| `pay_type` | 1支付宝 40%，2微信 40%，3银行卡 20% |
| `pay_time` | 已支付状态在 `create_time` 后 0~30 分钟 |
| `ship_time` | 已发货状态在 `pay_time` 后 2~48 小时 |
| `receive_time` | 已完成状态在 `ship_time` 后 24~120 小时 |
| `create_time` | 按上述日单量分布到 730 天内 |
| `update_time` | 取状态链最后一个时间点 |
| `address_id` | 随机 BIGINT |
| `remark` | 20% 有内容，其余为空 |
| `etl_time` | 固定 |

**生成方式**：按天循环生成，确保 `user_id` 对应的 `register_time <= order.create_time`。

---

### 4. `ods_order_items` — ODS 订单明细源数据表

**数据量：约 64.5 万条**

平均每单约 1.36 件商品。

| 字段 | 生成策略 |
|------|----------|
| `item_id` | 自增 BIGINT |
| `order_id` | 关联 `ods_orders` |
| `product_id` | 从 `status=1` 的商品中优先采样 |
| `product_name` | 取对应 `product_id` 的名称快照 |
| `quantity` | 70% 为 1，25% 为 2，5% 为 3+ |
| `unit_price` | 取商品 `price` 快照，允许 ±10% 促销波动 |
| `total_amount` | `quantity * unit_price` |

**生成方式**：遍历所有订单，每单随机生成 1~4 个 item。

---

## 二、供应链域 ODS

### 5. `ods_warehouses` — ODS 仓库源数据表

**数据量：15 条**

| 字段 | 生成策略 |
|------|----------|
| `warehouse_id` | 1 ~ 15 |
| `warehouse_name` | 预定义仓库名称（华北/华东/华南中心等 + 前置仓） |
| `warehouse_type` | central(3), region(8), front(4) |
| `province_id` / `city_id` | 随机分配到 15 个不同城市 |
| `capacity` | 10 万 ~ 500 万件 |
| `manager_name` | Faker 中文名 |
| `status` | 90% 启用(1)，10% 停用(0) |
| `etl_time` | 固定 |

**生成方式**：直接批量生成。

---

### 6. `ods_suppliers` — ODS 供应商源数据表

**数据量：80 条**

| 字段 | 生成策略 |
|------|----------|
| `supplier_id` | 1 ~ 80 |
| `supplier_name` | Faker 公司名或 `供应商_{id}` |
| `supplier_level` | A(20%), B(40%), C(30%), D(10%) |
| `cooperation_status` | 1合作中 70%，0终止 20%，2待审核 10% |
| `payment_terms` | 15/30/45/60/90 天，加权分布 |
| `province_id` | 随机 |
| `etl_time` | 固定 |

**生成方式**：直接批量生成 80 条。

---

### 7. `ods_inventory` — ODS 库存源数据表

**数据量：约 2.8 万条**

库存记录数 = 启用状态的仓库 × 覆盖的 SKU。15 个仓库每个覆盖约 1,500~2,200 个 SKU。

| 字段 | 生成策略 |
|------|----------|
| `inventory_id` | 自增 BIGINT |
| `warehouse_id` | 1 ~ 15（启用状态） |
| `product_id` | 1 ~ 3,000（上架商品） |
| `quantity` | 100 ~ 5,000 随机 |
| `available_qty` | `quantity * (0.6 ~ 0.95)` |
| `reserved_qty` | `quantity * (0 ~ 0.2)` |
| `in_transit_qty` | `quantity * (0 ~ 0.15)` |
| `update_time` | 近 30 天内随机 |
| `etl_time` | 固定 |

**生成方式**：为每个仓库随机分配 1,300~2,200 个 SKU，确保 `warehouse_id + product_id` 唯一。

---

### 8. `ods_purchase_orders` — ODS 采购订单源数据表

**数据量：约 5,500 条**

基于 80 家供应商和 730 天的采购周期生成。早期采购量低，后期逐步增长。

| 字段 | 生成策略 |
|------|----------|
| `po_id` | 自增 BIGINT |
| `supplier_id` | 从 `cooperation_status=1` 的供应商中采样 |
| `warehouse_id` | 从启用状态的仓库中采样 |
| `po_date` | 2024-04-16 ~ 2026-04-16 均匀分布 |
| `total_amount` | 5,000 ~ 500,000 元 |
| `status` | 3已入库 60%，2已发货 20%，1待确认 15%，4已取消 5% |
| `delivery_date` | `po_date + 3~15 天` |
| `create_time` | `po_date` 的 09:00~18:00 随机 |
| `etl_time` | 固定 |

**生成方式**：按月分配采购单配额，关联活跃供应商和仓库。

---

## 三、SaaS 域 ODS

### 9. `ods_subscriptions` — ODS 订阅源数据表

**数据量：约 2.9 万条**

从 10 万用户中抽取约 2 万作为 SaaS 付费客户。每个客户平均生成约 1.4 条订阅记录（含试用、升级、取消、重订等历史）。

| 字段 | 生成策略 |
|------|----------|
| `subscription_id` | 自增 BIGINT |
| `customer_id` | 从 10 万用户中采样出的 2 万 SaaS 客户 |
| `plan_id` | 1~5（免费/基础/专业/企业/定制） |
| `status` | 1活跃 55%，2取消 25%，3暂停 10%，4过期 10% |
| `mrr` | 按 plan 定价：0/99/299/999/4999，允许 ±10% 波动 |
| `start_date` | 首次订阅在注册后 0~30 天；后续订阅在上一条结束后 0~90 天 |
| `end_date` | 活跃状态设为远期日期；取消/过期状态设为明确结束日期 |
| `channel_id` | 1 ~ 8（获客渠道） |
| `create_time` | `start_date` 的随机时刻 |
| `etl_time` | 固定 |

**生成方式**：为每个 SaaS 客户按时间轴生成订阅链。

---

### 10. `ods_payments` — ODS 付款源数据表

**数据量：约 25.7 万条**

基于订阅记录生成付款流水。约 20% 为年付，80% 为月付。

| 字段 | 生成策略 |
|------|----------|
| `payment_id` | 自增 BIGINT |
| `customer_id` | 关联订阅的 `customer_id` |
| `subscription_id` | 关联具体订阅 |
| `amount` | 月付取 `mrr`；年付取 `mrr * 12`（享少量折扣） |
| `currency` | 90% CNY，10% USD |
| `payment_date` | 订阅有效期内按月/按年生成的付款日，允许 ±3 天偏移 |
| `payment_method` | alipay 40%，wechat 35%，bank_card 20%，other 5% |
| `status` | 1成功 92%，2失败 5%，3退款 3% |
| `etl_time` | 固定 |

**生成方式**：遍历所有订阅，按周期生成付款记录。退款金额为部分或全部负值。

---

### 11. `ods_tickets` — ODS 工单源数据表

**数据量：6 万条**

假设 2 万 SaaS 客户，平均每人 3 张客服工单。

| 字段 | 生成策略 |
|------|----------|
| `ticket_id` | 自增 BIGINT |
| `customer_id` | 从 SaaS 客户中随机采样 |
| `ticket_type` | technical 40%，billing 20%，sales 15%，general 25% |
| `priority` | low 30%，medium 45%，high 20%，urgent 5% |
| `status` | resolved 60%，closed 25%，pending 10%，open 5% |
| `create_date` | 2024-04-16 ~ 2026-04-16 均匀分布 |
| `resolve_date` | 已解决/已关闭的工单在 `create_date + 1~7 天` |
| `csat_score` | 已解决/已关闭的工单随机 1~5 分，均值约 3.8 |
| `etl_time` | 固定 |

**生成方式**：按天分配工单配额，直接生成 6 万条。

---

### 12. `ods_app_events` — ODS 应用事件表

**数据量：10 万条**

人为限制为 10 万条。日期集中在**近 90 天**（2026-01-16 ~ 2026-04-16），模拟近期产品活跃。

| 字段 | 生成策略 |
|------|----------|
| `event_id` | 自增 BIGINT |
| `user_id` | 活跃用户（近 30 天登录）权重更高 |
| `event_name` | login 30%，page_view 50%，feature_use 15%，error 3%，signup 2% |
| `event_date` | 近 90 天内随机 |
| `event_time` | login 事件集中在 09:00 和 20:00；其他事件全天随机 |
| `device_type` | ios 30%，android 25%，web 35%，miniapp 10% |
| `session_id` | `sess_` + 随机 8 位数字 |
| `etl_time` | 固定 |

**生成方式**：批量生成 10 万条，日期和时段按事件类型加权。

---

## 实际生成汇总

执行 `python workspace/dbgen/ods_generate_data.py` 后的实际数据量：

| 表名 | 域 | 实际行数 |
|------|----|---------|
| `ods_users` | 电商 | 100,000 |
| `ods_products` | 电商 | 3,000 |
| `ods_orders` | 电商 | 475,000 |
| `ods_order_items` | 电商 | 645,267 |
| `ods_warehouses` | 供应链 | 15 |
| `ods_suppliers` | 供应链 | 80 |
| `ods_inventory` | 供应链 | 27,856 |
| `ods_purchase_orders` | 供应链 | 5,515 |
| `ods_subscriptions` | SaaS | 28,642 |
| `ods_payments` | SaaS | 257,078 |
| `ods_tickets` | SaaS | 60,000 |
| `ods_app_events` | SaaS | 100,000 |

---

## 后续规划

- DIM 层数据（如 `dim_date`、`dim_region`、`dim_product`、`dim_plan` 等）将通过代码直接生成或引用 ODS 层主数据。
- DWD / DWS / ADS 层数据将基于 ODS 层数据通过 ETL 脚本生成，不直接由本脚本负责。
