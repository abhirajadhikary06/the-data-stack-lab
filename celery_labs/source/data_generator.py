import os
import csv
import time
import random
from faker import Faker
from datetime import datetime, timedelta

fake = Faker()

# Configuration
CSV_FILE = 'ecommerce_data.csv'
HEADERS = ['order_id', 'customer_name', 'product', 'category', 'price', 'quantity', 'order_date', 'status']
PRODUCTS = {
    'Electronics': ['Laptop', 'Smartphone', 'Headphones', 'Smartwatch'],
    'Home': ['Coffee Maker', 'Desk Lamp', 'Toaster', 'Vacuum'],
    'Apparel': ['T-Shirt', 'Hoodie', 'Sneakers', 'Jeans']
}

def generate_fake_data(num_records=10):
    data = []
    for _ in range(num_records):
        category = random.choice(list(PRODUCTS.keys()))
        product = random.choice(PRODUCTS[category])
        
        order_id = fake.uuid4()
        customer_name = fake.name()
        price = round(random.uniform(10.0, 1000.0), 2)
        quantity = random.randint(1, 5)
        order_date = (datetime.now() - timedelta(minutes=random.randint(0, 59))).strftime('%Y-%m-%d %H:%M:%S')
        status = random.choices(['Shipped', 'Processing', 'Cancelled'], weights=[0.8, 0.15, 0.05])[0]
        
        data.append([order_id, customer_name, product, category, price, quantity, order_date, status])
    return data

def append_to_csv(data):
    try:
        file_exists = os.path.isfile(CSV_FILE)
        with open(CSV_FILE, mode='a', newline='', encoding='utf-8') as file:
            writer = csv.writer(file)
            if not file_exists:
                writer.writerow(HEADERS)
            writer.writerows(data)
    except PermissionError:
        print(f"Error: Could not write to {CSV_FILE}. Ensure the file is not open in another program.")

def main():
    print(f"Starting data generation... Writing to {CSV_FILE}")
    try:
        while True:
            fake_data = generate_fake_data(10)
            append_to_csv(fake_data)
            
            current_time = datetime.now().strftime('%H:%M:%S')
            print(f"[{current_time}] Successfully added 10 records.")
            
            time.sleep(30)
    except KeyboardInterrupt:
        print("\nScript stopped by user.")

if __name__ == '__main__':
    main()
