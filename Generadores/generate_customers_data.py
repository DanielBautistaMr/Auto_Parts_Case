import boto3
from faker import Faker
import random
import json
import csv
import io
from datetime import datetime

# Configuración de S3
bucket_name = "data-lake-simulacion"
s3_client = boto3.client('s3')

# Ruta del archivo en S3
customers_file = "data/json/customers.json"
transactions_file = "data/csv/transactions.csv"

# Inicializar Faker
fake = Faker()

# Cargar nombres de productos desde el archivo JSON
with open("product_names.json", "r") as f:
    product_names = json.load(f)

# Lista para almacenar transacciones compartidas
shared_transactions = []

def generate_customer_data():
    """Genera un cliente único con una transacción que incluye múltiples productos."""
    customer_id = str(fake.uuid4())
    customer_name = fake.name()
    customer_email = fake.email()
    region = fake.country()

    # Generar una transacción única con múltiples productos
    transaction_id = str(fake.uuid4())
    transaction_date = fake.date_time_this_year().strftime("%Y-%m-%d %H:%M:%S")
    products = []
    total_amount = 0

    for _ in range(random.randint(1, 5)):
        category = random.choice(list(product_names.keys()))
        product_name = random.choice(product_names[category])
        amount = round(random.uniform(20.0, 2000.0), 2)
        total_amount += amount
        products.append({
            "product_name": product_name,
            "category": category,
            "amount": amount
        })

    # Crear la transacción
    transaction = {
        "transaction_id": transaction_id,
        "customer_id": customer_id,
        "transaction_date": transaction_date,
        "products": products,
        "total_amount": round(total_amount, 2)
    }

    # Guardar la transacción en shared_transactions
    shared_transactions.append(transaction)

    # Crear los datos del cliente
    customer_data = {
        "customer_id": customer_id,
        "customer_name": customer_name,
        "customer_email": customer_email,
        "region": region,
        "total_spent": round(total_amount, 2),
        "transaction": transaction
    }

    return customer_data

def upload_to_s3(data, filename, format_type="json"):
    """Sube datos a S3 en el formato especificado."""
    if format_type == "json":
        try:
            existing_data = []
            try:
                response = s3_client.get_object(Bucket=bucket_name, Key=filename)
                existing_data = json.loads(response["Body"].read().decode("utf-8"))
            except s3_client.exceptions.NoSuchKey:
                pass  # Si no existe, empieza con una lista vacía

            combined_data = existing_data + data
            s3_client.put_object(Bucket=bucket_name, Key=filename, Body=json.dumps(combined_data, indent=4))
            print(f"Datos subidos a S3: {filename}")
        except Exception as e:
            print(f"Error al subir {filename} a S3: {e}")
    elif format_type == "csv":
        output = io.StringIO()
        fieldnames = ["transaction_id", "customer_id", "transaction_date", "product_name", "category", "amount", "total_amount"]
        writer = csv.DictWriter(output, fieldnames=fieldnames)

        writer.writeheader()
        for transaction in data:
            for product in transaction["products"]:
                writer.writerow({
                    "transaction_id": transaction["transaction_id"],
                    "customer_id": transaction["customer_id"],
                    "transaction_date": transaction["transaction_date"],
                    "product_name": product["product_name"],
                    "category": product["category"],
                    "amount": product["amount"],
                    "total_amount": transaction["total_amount"]
                })

        try:
            s3_client.put_object(Bucket=bucket_name, Key=filename, Body=output.getvalue())
            print(f"Datos subidos a S3: {filename}")
        except Exception as e:
            print(f"Error al subir {filename} a S3: {e}")

def main():
    print("Generando datos de clientes y transacciones...")
    customers = [generate_customer_data() for _ in range(200)]

    # Subir datos a S3
    upload_to_s3(customers, customers_file, "json")
    upload_to_s3(shared_transactions, transactions_file, "csv")

    print("Datos de clientes y transacciones generados y subidos correctamente.")

if __name__ == "__main__":
    main()
