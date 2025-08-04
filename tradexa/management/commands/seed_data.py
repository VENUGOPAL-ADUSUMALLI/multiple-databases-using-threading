import threading
from django.core.management.base import BaseCommand
from django.db import connections
import time

# Data for insertion
USERS_DATA = [
    (1, "Alice", "alice@example.com"),
    (2, "Bob", "bob@example.com"),
    (3, "Charlie", "charlie@example.com"),
    (4, "David", "david@example.com"),
    (5, "Eve", "eve@example.com"),
    (6, "Frank", "frank@example.com"),
    (7, "Grace", "grace@example.com"),
    (8, "Alice", "alice@example.com"),
    (9, "Henry", "henry@example.com"),
    (10, "", "jane@example.com"),
]

PRODUCTS_DATA = [
    (1, "Laptop", 1000.00),
    (2, "Smartphone", 700.00),
    (3, "Headphones", 150.00),
    (4, "Monitor", 300.00),
    (5, "Keyboard", 50.00),
    (6, "Mouse", 30.00),
    (7, "Laptop", 1000.00),
    (8, "Smartwatch", 250.00),
    (9, "Gaming Chair", 500.00),
    (10, "Earbuds", -50.00),
]

ORDERS_DATA = [
    (1, 1, 1, 2),
    (2, 2, 2, 1),
    (3, 3, 3, 5),
    (4, 4, 4, 1),
    (5, 5, 5, 3),
    (6, 6, 6, 4),
    (7, 7, 7, 2),
    (8, 8, 8, 0),
    (9, 9, 1, -1),
    (10, 10, 11, 2),
]

print_lock = threading.Lock()


def thread_safe_print(message):
    with print_lock:
        print(message)


# Inserting users into users.db
def insert_users():
    thread_name = threading.current_thread().name
    connection = connections['users']

    with connection.cursor() as cursor:
        for user in USERS_DATA:
            start_time = time.time()
            try:
                # validations for user_Data
                if not user[1] or not user[1].strip():
                    raise ValueError("Name cannot be empty")
                if not user[2] or not user[2].strip():
                    raise ValueError("Email cannot be empty")
                if '@' not in user[2]:
                    raise ValueError("Invalid email format")

                cursor.execute("INSERT INTO users (id, name, email) VALUES (%s, %s, %s)", user)
                connection.commit()

                end_time = time.time()
                thread_safe_print(
                    f"[{thread_name}] User SUCCESS: ID={user[0]}, Name='{user[1]}', Email='{user[2]}' ({end_time - start_time:.3f}s)")

            except Exception as e:
                thread_safe_print(f"[{thread_name}] User FAILED: ID={user[0]} -> {e}")


# Inserting products into products.db
def insert_products():
    thread_name = threading.current_thread().name
    connection = connections['products']

    with connection.cursor() as cursor:
        for product in PRODUCTS_DATA:
            start_time = time.time()
            try:
                # validations for product data
                if not product[1] or not product[1].strip():
                    raise ValueError("Product name cannot be empty")
                if product[2] < 0:
                    raise ValueError(f"Price cannot be negative (${product[2]})")

                cursor.execute("INSERT INTO products (id, name, price) VALUES (%s, %s, %s)", product)
                connection.commit()

                end_time = time.time()
                thread_safe_print(
                    f"[{thread_name}] Product SUCCESS: ID={product[0]}, Name='{product[1]}', Price=${product[2]} ({end_time - start_time:.3f}s)")

            except Exception as e:
                thread_safe_print(f"[{thread_name}] Product FAILED: ID={product[0]} -> {e}")


# Inserting orders into orders.db
def insert_orders():
    thread_name = threading.current_thread().name
    connection = connections['orders']

    with connection.cursor() as cursor:
        for order in ORDERS_DATA:
            start_time = time.time()
            try:
                # validations for orders
                if order[3] <= 0:
                    raise ValueError(f"Quantity must be positive (got {order[3]})")
                if order[1] <= 0:
                    raise ValueError(f"Invalid user_id ({order[1]})")
                if order[2] <= 0:
                    raise ValueError(f"Invalid product_id ({order[2]})")
                cursor.execute("INSERT INTO orders (id, user_id, product_id, quantity) VALUES (%s, %s, %s, %s)", order)
                connection.commit()

                end_time = time.time()
                thread_safe_print(
                    f"[{thread_name}] Order SUCCESS: ID={order[0]}, UserID={order[1]}, ProductID={order[2]}, Qty={order[3]} ({end_time - start_time:.3f}s)")

            except Exception as e:
                thread_safe_print(f"[{thread_name}] Order FAILED: ID={order[0]} -> {e}")


def show_database_contents():

    thread_safe_print("\n--- USERS DATABASE (users.db) ---")
    try:
        with connections['users'].cursor() as cursor:
            cursor.execute("SELECT * FROM users ORDER BY id")
            users = cursor.fetchall()
            if users:
                thread_safe_print(f"{'ID':<3} {'Name':<10} {'Email':<25}")
                thread_safe_print("-" * 40)
                for user in users:
                    thread_safe_print(f"{user[0]:<3} {user[1]:<10} {user[2]:<25}")
            else:
                thread_safe_print("No users found.")
    except Exception as e:
        thread_safe_print(f"Error reading users: {e}")

    thread_safe_print("\n--- PRODUCTS DATABASE (products.db) ---")
    try:
        with connections['products'].cursor() as cursor:
            cursor.execute("SELECT * FROM products ORDER BY id")
            products = cursor.fetchall()
            if products:
                thread_safe_print(f"{'ID':<3} {'Name':<15} {'Price':<10}")
                thread_safe_print("-" * 30)
                for product in products:
                    thread_safe_print(f"{product[0]:<3} {product[1]:<15} ${product[2]:<9.2f}")
            else:
                thread_safe_print("No products found.")
    except Exception as e:
        thread_safe_print(f"Error reading products: {e}")

    thread_safe_print("\n--- ORDERS DATABASE (orders.db) ---")
    try:
        with connections['orders'].cursor() as cursor:
            cursor.execute("SELECT * FROM orders ORDER BY id")
            orders = cursor.fetchall()
            if orders:
                thread_safe_print(f"{'ID':<3} {'UserID':<7} {'ProductID':<10} {'Quantity':<8}")
                thread_safe_print("-" * 30)
                for order in orders:
                    thread_safe_print(f"{order[0]:<3} {order[1]:<7} {order[2]:<10} {order[3]:<8}")
            else:
                thread_safe_print("No orders found.")
    except Exception as e:
        thread_safe_print(f"Error reading orders: {e}")


class Command(BaseCommand):
    help = "Insert test data concurrently into distributed databases"

    def handle(self, *args, **kwargs):
        self.stdout.write("Starting concurrent database insertions...")
        self.stdout.write("=" * 60)

        start_time = time.time()

        t1 = threading.Thread(target=insert_users, name="UserThread")
        t2 = threading.Thread(target=insert_products, name="ProductThread")
        t3 = threading.Thread(target=insert_orders, name="OrderThread")

        t1.start()
        t2.start()
        t3.start()

        t1.join()
        t2.join()
        t3.join()

        end_time = time.time()

        show_database_contents()

        thread_safe_print(f"\n" + "=" * 50)
        thread_safe_print("EXECUTION SUMMARY")
        thread_safe_print("=" * 50)
        thread_safe_print(f"Total execution time: {end_time - start_time:.3f} seconds")
        thread_safe_print(f"Total records attempted: {len(USERS_DATA) + len(PRODUCTS_DATA) + len(ORDERS_DATA)}")

        self.stdout.write(self.style.SUCCESS("Concurrent data insertion complete!"))
