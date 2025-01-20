import boto3
import json
import random
from faker import Faker

# Configuración de S3
bucket_name = "data-lake-simulacion"  # Nombre del bucket S3
s3_client = boto3.client('s3')

# Nombre del archivo en S3
inventory_file = "data/json/inventories.json"
transactions_file = "data/json/transactions.json"

# Inicializar Faker
fake = Faker()

# Productos cargados desde el JSON
with open("product_names.json", "r") as f:
    product_names = json.load(f)

categories = list(product_names.keys())

def generate_inventory_data():
    """Genera un registro único de inventario."""
    warehouse_id = fake.uuid4()
    category = random.choice(categories)
    product_name = random.choice(product_names[category])

    # Introducir suciedades en los datos
    product_name = product_name if random.random() > 0.1 else product_name.lower()  # Producto en minúsculas
    quantity = random.randint(0, 500) if random.random() > 0.15 else random.choice([None, -random.randint(1, 20)])
    last_updated = (
        fake.date_time_this_year().strftime("%Y-%m-%d %H:%M:%S")
        if random.random() > 0.05
        else "INVALID_DATE"
    )

    inventory_data = {
        "warehouse_id": warehouse_id,
        "product_name": product_name,
        "quantity": quantity,
        "last_updated": last_updated
    }

    return inventory_data

def generate_associated_data(inventories):
    """Genera datos asociados para asegurarse de que las propiedades coincidan entre fuentes."""
    transactions = []
    for inventory in inventories:
        # Asociar transacciones a los productos de inventario
        if inventory["quantity"] and inventory["quantity"] > 0:
            transactions.append({
                "transaction_id": fake.uuid4(),
                "warehouse_id": inventory["warehouse_id"],
                "product_name": inventory["product_name"],
                "transaction_date": fake.date_time_this_year().strftime("%Y-%m-%d %H:%M:%S"),
                "quantity_sold": random.randint(1, min(inventory["quantity"], 50)),
                "amount": round(random.uniform(20.0, 2000.0), 2)
            })
    return transactions

def upload_to_s3(data, filename):
    """Sube datos a S3 en formato JSON."""
    # Convertir los datos a JSON
    json_data = json.dumps(data, indent=4)

    # Subir a S3
    try:
        s3_client.put_object(Bucket=bucket_name, Key=filename, Body=json_data)
        print(f"Datos subidos a S3: {filename}")
    except Exception as e:
        print(f"Error al subir {filename} a S3: {e}")

def main():
    print("Generando datos de inventarios y datos asociados...")
    inventories = []

    # Generar 300 registros de inventario
    for _ in range(300):
        inventory_data = generate_inventory_data()
        inventories.append(inventory_data)

    # Generar datos asociados (transacciones basadas en inventarios)
    transactions = generate_associated_data(inventories)

    # Subir datos a S3 en formato JSON
    upload_to_s3(inventories, inventory_file)
    upload_to_s3(transactions, transactions_file)
    print("Datos de inventarios y transacciones generados y subidos a S3 correctamente.")

if __name__ == "__main__":
    main()
