import os
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv

load_dotenv()

db_user = os.getenv("DB_USER")
db_password = os.getenv("DB_PASSWORD")
db_host = os.getenv("DB_HOST")
db_port = os.getenv("DB_PORT")
db_name = os.getenv("DB_NAME")
db_url = f"postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
engine = create_engine(db_url)

try:
    with engine.connect() as connection:
        print("Connexion réussie à la base de données !")
except Exception as e:
    print(f"Erreur de connexion ou de création de tables: {e}")

datalake = './DataLake'
customers_dir = os.path.join(datalake, 'customers')
df_customers = pd.read_csv(os.path.join(customers_dir, 'olist_customers_dataset.csv'))
geolocation_dir = os.path.join(datalake, 'geolocation')
df_geolocation = pd.read_csv(os.path.join(geolocation_dir, 'olist_geolocation_dataset.csv'))
sellers_dir = os.path.join(datalake, 'sellers')
df_sellers = pd.read_csv(os.path.join(sellers_dir, 'olist_sellers_dataset.csv'))
products_dir = os.path.join(datalake, 'products')
df_products = pd.read_csv(os.path.join(products_dir, 'olist_products_dataset.csv'))
orders_dir = os.path.join(datalake, 'orders')
df_order_items = pd.read_csv(os.path.join(orders_dir, 'olist_order_items_dataset.csv'))
df_payments = pd.read_csv(os.path.join(orders_dir, 'olist_order_payments_dataset.csv'))

print(df_customers.head())
print(df_geolocation.head())

print(df_customers.columns)
print(df_customers.describe())
print(df_customers.info())

# print(df_geolocation.columns)
# print(df_geolocation.describe())
# print(df_geolocation.info())

# Garder les colonnes utiles
df_customers = df_customers[["customer_id", "customer_unique_id", "customer_city", "customer_state"]]

# Insérer les données du df_customers dans la table dim_customers
df_customers.to_sql('dim_customers', engine, if_exists='replace', index=False)

print(df_sellers.columns)
print(df_sellers.describe())
print(df_sellers.info())

df_sellers = df_sellers[["seller_id", "seller_city", "seller_state"]]

df_sellers = df_sellers.drop_duplicates()
df_sellers.to_sql('dim_sellers', engine, if_exists='append', index=False)

# # Pour les produits

print(df_products.columns)
print(df_products.describe())
print(df_products.info())

# Pour avoir une ligne pour chaque produit
print(len(df_order_items))
df_order_items = df_order_items.drop_duplicates(subset=["order_id", "product_id"], keep="last")
print(len(df_order_items))

print(df_order_items.columns)
print(df_order_items.describe())
print(df_order_items.info())

df_products['product_id'] = df_products['product_id'].astype(str)
df_order_items['product_id'] = df_order_items['product_id'].astype(str)
df_products = df_products[["product_id", "product_category_name"]]
df_final_products = df_products.merge(df_order_items[["product_id", "price"]], on=['product_id'], how='left')

# Supprimer les doublons
df_final_products = df_final_products.drop_duplicates()
print(df_final_products.head(10))

df_final_products.to_sql('dim_products', engine, if_exists='replace', index=False)

# Pour le time
df_orders = pd.read_csv(os.path.join(orders_dir, 'olist_orders_dataset.csv'))
df_time = df_orders[["order_id", "customer_id", "order_approved_at", "order_delivered_customer_date"]]
df_time = df_time.drop_duplicates()

df_time.to_sql('dim_time', engine, if_exists='replace', index=False)

# Table de fait

df_fact_sales = df_orders[["order_id", "customer_id"]].merge(df_customers[["customer_id"]], how="left", on="customer_id")
df_fact_sales = df_fact_sales.merge(df_order_items[['seller_id', 'product_id', 'order_id']], how="left", on="order_id")
df_fact_sales = df_fact_sales.merge(df_payments[["payment_value", "order_id"]], how="left", on="order_id")
df_fact_sales = df_fact_sales.merge(df_order_items[["order_item_id", "order_id"]], how="left", on="order_id")
df_fact_sales['date_id'] = df_fact_sales['order_id']
df_fact_sales['fact_sales_id'] = df_fact_sales['order_id'].astype(str)+"_"+df_fact_sales['customer_id'].astype(str)+"_"+df_fact_sales['seller_id'].astype(str)+"_"+df_fact_sales['product_id'].astype(str)+"_"+df_fact_sales['payment_value'].astype(str)+"_"+df_fact_sales['order_item_id'].astype(str)

print(df_fact_sales.columns)
print(df_fact_sales.head())

df_fact_sales.to_sql('fact_sales', engine, if_exists='replace', index=False)