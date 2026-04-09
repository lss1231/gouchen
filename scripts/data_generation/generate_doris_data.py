"""Generate mock data for Doris warehouse layers."""
import os
import sys
import random
from datetime import datetime, timedelta
from tqdm import tqdm

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import pymysql
from src.config import get_settings

# Data scale configuration
SCALE = {
    'users': 100_000,
    'products': 10_000,
    'orders': 100_000,
    'order_items': 350_000,
}

# Constants
PROVINCES = ['北京', '上海', '广东', '浙江', '江苏', '四川', '湖北', '河南', '山东', '河北']
BRANDS = ['苹果', '华为', '小米', '海尔', '美的', '联想', '戴尔', '耐克', '阿迪', '优衣库']


def get_connection():
    """Get Doris connection."""
    s = get_settings()
    return pymysql.connect(
        host=s.doris_host, port=s.doris_port,
        user=s.doris_user, password=s.doris_password,
        database=s.doris_database, charset='utf8mb4'
    )


def batch_insert(conn, sql, records, batch_size=3000):
    """Batch insert records."""
    cursor = conn.cursor()
    for i in range(0, len(records), batch_size):
        batch = records[i:i + batch_size]
        cursor.executemany(sql, batch)
        conn.commit()


def generate_ods_users():
    """Generate ODS users: 100k."""
    print("Generating ODS users...")
    conn = get_connection()

    records = []
    base_time = datetime.now()
    for i in tqdm(range(SCALE['users']), desc="Users"):
        reg_time = datetime(2023, 1, 1) + timedelta(days=random.randint(0, 700))
        records.append((
            i + 1, f"user{i+1}", f"user{i+1}@test.com", f"138{i:08d}"[:11],
            random.randint(0, 1),  # gender
            (datetime(1980, 1, 1) + timedelta(days=random.randint(0, 15000))).strftime('%Y-%m-%d'),  # birthday
            reg_time.strftime('%Y-%m-%d %H:%M:%S'),
            (reg_time + timedelta(days=random.randint(1, 100))).strftime('%Y-%m-%d %H:%M:%S'),
            1,  # user_status
            random.randint(1, 300),  # city_id
            random.randint(1, 35),  # province_id
            base_time.strftime('%Y-%m-%d %H:%M:%S')
        ))

    sql = '''INSERT INTO ods_users
        (user_id, username, email, phone, gender, birthday, register_time, last_login_time,
         user_status, city_id, province_id, etl_time)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'''
    batch_insert(conn, sql, records)
    conn.close()
    print(f"  Generated {len(records)} users")


def generate_ods_products():
    """Generate ODS products: 10k."""
    print("Generating ODS products...")
    conn = get_connection()

    records = []
    base_time = datetime.now()
    for i in tqdm(range(SCALE['products']), desc="Products"):
        create_time = datetime(2023, 1, 1) + timedelta(days=random.randint(0, 700))
        price = round(random.uniform(50, 5000), 2)
        records.append((
            i + 1, f"商品{i+1}", random.randint(1, 50), random.randint(1, 10),
            price, round(price * 0.6, 2), random.randint(100, 10000),
            1,  # status
            create_time.strftime('%Y-%m-%d %H:%M:%S'),
            create_time.strftime('%Y-%m-%d %H:%M:%S'),
            base_time.strftime('%Y-%m-%d %H:%M:%S')
        ))

    sql = '''INSERT INTO ods_products
        (product_id, product_name, category_id, brand_id, price, cost, stock, status,
         create_time, update_time, etl_time)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'''
    batch_insert(conn, sql, records)
    conn.close()
    print(f"  Generated {len(records)} products")


def generate_ods_orders_and_items():
    """Generate ODS orders (100k) and order items (350k)."""
    print("Generating ODS orders and items...")
    conn = get_connection()
    base_time = datetime.now()

    # Generate orders
    order_records = []
    for i in tqdm(range(SCALE['orders']), desc="Orders"):
        order_id = i + 1
        create_time = datetime(2024, 1, 1) + timedelta(days=random.randint(0, 364))
        total = round(random.uniform(100, 3000), 2)
        discount = round(total * random.uniform(0, 0.15), 2)
        pay = total - discount

        order_records.append((
            order_id, random.randint(1, SCALE['users']),
            random.randint(2, 4),  # status
            total, discount, pay,
            random.randint(1, 5),  # pay_type
            (create_time + timedelta(minutes=random.randint(1, 60))).strftime('%Y-%m-%d %H:%M:%S'),
            (create_time + timedelta(hours=random.randint(1, 24))).strftime('%Y-%m-%d %H:%M:%S') if random.random() > 0.1 else None,
            (create_time + timedelta(days=random.randint(2, 5))).strftime('%Y-%m-%d %H:%M:%S') if random.random() > 0.2 else None,
            random.randint(1, 100000),  # address_id
            '',  # remark
            create_time.strftime('%Y-%m-%d %H:%M:%S'),
            create_time.strftime('%Y-%m-%d %H:%M:%S'),
            base_time.strftime('%Y-%m-%d %H:%M:%S')
        ))

    sql = '''INSERT INTO ods_orders
        (order_id, user_id, order_status, total_amount, discount_amount, pay_amount,
         pay_type, pay_time, ship_time, receive_time, address_id, remark,
         create_time, update_time, etl_time)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'''
    batch_insert(conn, sql, order_records)
    print(f"  Generated {len(order_records)} orders")

    # Generate order items
    item_records = []
    item_id = 1
    for order_id in tqdm(range(1, SCALE['orders'] + 1), desc="Items"):
        num_items = random.randint(1, 5)
        for _ in range(num_items):
            unit_price = round(random.uniform(50, 2000), 2)
            qty = random.randint(1, 3)
            item_records.append((
                item_id, order_id, random.randint(1, SCALE['products']),
                f"商品{random.randint(1, SCALE['products'])}",
                qty, unit_price, round(unit_price * qty, 2)
            ))
            item_id += 1
            if len(item_records) >= SCALE['order_items']:
                break
        if len(item_records) >= SCALE['order_items']:
            break

    sql = '''INSERT INTO ods_order_items
        (item_id, order_id, product_id, product_name, quantity, unit_price, total_amount)
        VALUES (%s, %s, %s, %s, %s, %s, %s)'''
    batch_insert(conn, sql, item_records)
    conn.close()
    print(f"  Generated {len(item_records)} order items")


def verify_data():
    """Verify data counts."""
    print("\nVerifying data counts...")
    conn = get_connection()
    cursor = conn.cursor()

    tables = ['ods_users', 'ods_products', 'ods_orders', 'ods_order_items']
    for t in tables:
        cursor.execute(f'SELECT COUNT(*) FROM {t}')
        count = cursor.fetchone()[0]
        print(f'  {t}: {count:,}')

    conn.close()


def main():
    """Main entry."""
    print("=" * 60)
    print("Doris Data Generation")
    print("=" * 60)

    generate_ods_users()
    generate_ods_products()
    generate_ods_orders_and_items()
    verify_data()

    print("\n" + "=" * 60)
    print("Data generation completed!")
    print("=" * 60)


if __name__ == '__main__':
    main()
