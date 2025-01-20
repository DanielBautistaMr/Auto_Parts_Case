import boto3
from faker import Faker
import random
import json
import csv
import time
from datetime import datetime
import io

# Configuración de S3
bucket_name = "data-lake-simulacion"  # Nombre del bucket S3
s3_client = boto3.client('s3')

# Carpetas específicas por tipo de archivo
folders = {
    "json": "data/json/",
    "csv": "data/csv/",
}

# Archivos en sus carpetas respectivas
file_paths = {
    "customers": folders["json"] + "customers.json",
    "transactions": folders["csv"] + "transactions.csv",
    "providers": folders["csv"] + "providers.csv",
    "products": folders["json"] + "products.json",  # Cambiado de XML a JSON
    "invoices": folders["json"] + "invoices.json",  # Cambiado de Parquet a JSON
}

# Cargar nombres de productos desde el archivo JSON
with open("product_names.json", "r") as f:
    product_names = json.load(f)

# Inicializar Faker
fake = Faker()

# Generar datos comunes
shared_transactions = []

def read_existing_data(filename, format_type):
    """Lee datos existentes desde S3 y los retorna."""
    try:
        response = s3_client.get_object(Bucket=bucket_name, Key=filename)
        content = response["Body"].read()
        if format_type == "json":
            return json.loads(content.decode("utf-8"))
        elif format_type == "csv":
            csv_content = content.decode("utf-8")
            reader = csv.DictReader(io.StringIO(csv_content))
            return list(reader), reader.fieldnames
        return []
    except s3_client.exceptions.NoSuchKey:
        return []
    except Exception as e:
        print(f"Error al leer {filename}: {e}")
        return []

def generate_customer_data():
    """Genera un cliente único con transacciones asociadas."""
    customer_id = fake.uuid4()
    customer_name = fake.name()
    customer_email = fake.user_name() + "@gmail.com"
    region = fake.country()

    # Generar transacciones asociadas al cliente
    purchase_history = []
    total_spent = 0
    for _ in range(random.randint(1, 5)):
        category = random.choice(list(product_names.keys()))
        product_name = random.choice(product_names[category])
        amount = round(random.uniform(20.0, 2000.0), 2)
        transaction_id = fake.uuid4()
        purchase_date = fake.date_time_this_year().strftime("%Y-%m-%d %H:%M:%S")
        total_spent += amount

        transaction = {
            "transaction_id": str(transaction_id),
            "product_name": product_name,
            "purchase_date": purchase_date,
            "amount": amount
        }
        purchase_history.append(transaction)

        # Ensure consistent field order for shared_transactions
        shared_transactions.append({
            "transaction_id": str(transaction_id),
            "customer_id": customer_id,
            "purchase_date": purchase_date,
            "product_name": product_name,
            "amount": str(amount)  # Convert to string for consistent CSV handling
        })

    customer_data = {
        "customer_id": customer_id,
        "customer_name": customer_name,
        "customer_email": customer_email,
        "region": region,
        "total_spent": round(total_spent, 2),
        "purchase_history": purchase_history
    }

    return customer_data

def generate_providers_data():
    """Genera un proveedor único."""
    providers = []
    for _ in range(50):
        provider_id = fake.uuid4()
        provider_name = fake.company()
        product_category = random.choice(list(product_names.keys()))
        product_name = random.choice(product_names[product_category])
        contact_email = fake.email()
        providers.append({
            "provider_id": provider_id,
            "provider_name": provider_name,
            "product_name": product_name,
            "contact_email": contact_email
        })
    return providers

def generate_products_data():
    """Genera un conjunto de productos en formato JSON."""
    products = []
    for category, products_list in product_names.items():
        for product in products_list:
            products.append({
                "category": category,
                "name": product,
                "price": round(random.uniform(20.0, 2000.0), 2)
            })
    return products

def generate_invoices_data():
    """Genera facturas en formato JSON."""
    invoices = []
    for transaction in shared_transactions:
        invoice_id = fake.uuid4()
        invoices.append({
            "invoice_id": str(invoice_id),
            "transaction_id": transaction["transaction_id"],
            "amount": float(transaction["amount"]),
            "invoice_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        })
    return invoices

def upload_to_s3(data, file_path, format_type="json"):
    """Sube datos a S3 organizados por carpetas."""
    if format_type == "json":
        existing_data = read_existing_data(file_path, format_type)
        data = existing_data + data
        data = json.dumps(data, indent=4)

    elif format_type == "csv":
        result = read_existing_data(file_path, format_type)
        if isinstance(result, tuple):
            existing_data, fieldnames = result
        else:
            existing_data = result
            if data and isinstance(data, list) and len(data) > 0:
                fieldnames = list(data[0].keys())
            else:
                fieldnames = []

        output = io.StringIO()
        if fieldnames:
            writer = csv.DictWriter(output, fieldnames=fieldnames)
            writer.writeheader()
            if existing_data:
                writer.writerows(existing_data)
            if data:
                writer.writerows(data)
            data = output.getvalue()

    try:
        s3_client.put_object(Bucket=bucket_name, Key=file_path, Body=data)
        print(f"Datos subidos a S3: {file_path}")
    except Exception as e:
        print(f"Error al subir {file_path} a S3: {e}")

def main():
    print("Iniciando generación continua de datos...")
    while True:
        # Limpiar transacciones compartidas antes de cada iteración
        shared_transactions.clear()

        # Generar y acumular datos
        customers = [generate_customer_data() for _ in range(200)]

        # Subir datos a S3
        upload_to_s3(customers, file_paths["customers"], "json")
        upload_to_s3(shared_transactions, file_paths["transactions"], "csv")
        upload_to_s3(generate_providers_data(), file_paths["providers"], "csv")
        upload_to_s3(generate_products_data(), file_paths["products"], "json")  # Cambiado a JSON
        upload_to_s3(generate_invoices_data(), file_paths["invoices"], "json")  # Cambiado a JSON

        print(f"Datos generados y subidos. Esperando 12 minutos...")
        time.sleep(12 * 60)

if __name__ == "__main__":
    main()
