import boto3
import csv
import io
from faker import Faker
import random
import json
from datetime import datetime

# Configuración de S3
bucket_name = "data-lake-simulacion"  # Nombre del bucket S3
s3_client = boto3.client('s3')
transactions_file = "data/csv/transactions.csv"  # Ruta actualizada para transacciones

# Inicializar Faker
fake = Faker()

# Cargar nombres de productos desde el archivo JSON
with open("product_names.json", "r") as f:
    product_names = json.load(f)

# Leer datos existentes desde S3
def read_existing_data(filename):
    """Lee datos existentes desde S3 y retorna una lista de diccionarios."""
    try:
        response = s3_client.get_object(Bucket=bucket_name, Key=filename)
        content = response["Body"].read().decode("utf-8")
        reader = csv.DictReader(io.StringIO(content))
        return list(reader)
    except s3_client.exceptions.NoSuchKey:
        # Si no existe el archivo, retornar una lista vacía
        return []
    except Exception as e:
        print(f"Error al leer {filename}: {e}")
        return []

# Subir transacciones a S3 en formato CSV
def upload_transactions_to_s3(transactions, filename):
    """Sube las transacciones a S3 en formato CSV, desglosando los productos."""
    output = io.StringIO()
    writer = csv.writer(output)

    # Encabezados: una fila por producto dentro de una transacción
    writer.writerow(["transaction_id", "customer_id", "transaction_date", "product_name", "category", "amount", "total_amount"])

    # Escribir datos
    for transaction in transactions:
        for product in transaction["products"]:
            writer.writerow([
                transaction["transaction_id"],
                transaction["customer_id"],
                transaction["transaction_date"],
                product["product_name"],
                product["category"],
                product["amount"],
                transaction["total_amount"]
            ])

    # Subir a S3
    try:
        s3_client.put_object(Bucket=bucket_name, Key=filename, Body=output.getvalue())
        print(f"Transacciones subidas a S3: {filename}")
    except Exception as e:
        print(f"Error al subir {filename} a S3: {e}")

# Generar transacciones
def generate_transactions(customers):
    """Genera transacciones asociadas con clientes."""
    transactions = []

    for customer in customers:
        customer_id = customer["customer_id"]
        transaction_id = str(fake.uuid4())
        transaction_date = fake.date_time_this_year().strftime("%Y-%m-%d %H:%M:%S")

        # Generar productos asociados a la transacción
        products = []
        total_amount = 0
        for _ in range(random.randint(1, 5)):  # Entre 1 y 5 productos por transacción
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
        transactions.append({
            "transaction_id": transaction_id,
            "customer_id": customer_id,
            "transaction_date": transaction_date,
            "products": products,
            "total_amount": total_amount
        })

    return transactions

# Programa principal
def main():
    print("Generando datos de transacciones...")

    # Leer clientes desde S3
    customers_file = "data/json/customers.json"  # Ruta actualizada para clientes
    try:
        response = s3_client.get_object(Bucket=bucket_name, Key=customers_file)
        customers = json.loads(response["Body"].read().decode("utf-8"))
    except Exception as e:
        print(f"Error al leer {customers_file}: {e}")
        return

    # Generar transacciones
    transactions = generate_transactions(customers)

    # Subir transacciones a S3
    upload_transactions_to_s3(transactions, transactions_file)
    print("Transacciones generadas y subidas correctamente.")

if __name__ == "__main__":
    main()
