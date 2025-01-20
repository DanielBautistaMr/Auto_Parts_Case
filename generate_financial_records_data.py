import boto3
import random
import json
from datetime import datetime, timedelta
from faker import Faker
import time

# Configuración de S3
bucket_name = "data-lake-simulacion"  # Nombre del bucket S3
s3_client = boto3.client('s3')

# Nombre del archivo en S3
receipts_file = "data/json/receipts.json"

# Inicializar Faker
fake = Faker()

def generate_receipt_data(transaction_data):
    """Genera un recibo basado en datos de transacciones."""
    receipts = []
    for transaction in transaction_data:
        receipt = {
            "transaction_id": transaction["transaction_id"],
            "customer_id": transaction["customer_id"],
            "receipt_id": fake.uuid4(),
            "amount": transaction["amount"],
            "receipt_date": (datetime.strptime(transaction["purchase_date"], "%Y-%m-%d %H:%M:%S") + timedelta(hours=random.randint(1, 72))).strftime("%Y-%m-%d %H:%M:%S"),
            "payment_method": random.choice(["Credit Card", "Debit Card", "Cash", "Mobile Payment"]),
            "notes": fake.sentence(nb_words=10) if random.random() > 0.8 else None  # Introduce "suciedad"
        }
        # Introducir inconsistencias en los datos
        if random.random() > 0.9:  # 10% de los datos tienen inconsistencias
            receipt["amount"] = round(receipt["amount"] * random.uniform(0.9, 1.1), 2)  # Ligeros errores en montos
        receipts.append(receipt)
    return receipts

def upload_to_s3(data, filename):
    """Sube datos a S3 en formato JSON."""
    json_data = json.dumps(data, indent=4)
    try:
        s3_client.put_object(Bucket=bucket_name, Key=filename, Body=json_data)
        print(f"Datos subidos a S3: {filename}")
    except Exception as e:
        print(f"Error al subir {filename} a S3: {e}")

def main():
    print("Generando datos de recibos...")
    # Simulación: cargar transacciones generadas previamente (mock)
    # En producción, cargaríamos las transacciones reales desde S3 o base de datos
    with open("transactions.json", "r") as f:  # Asegúrate de tener un archivo de transacciones local para la prueba
        transaction_data = json.load(f)

    receipts = generate_receipt_data(transaction_data)

    # Subir recibos a S3
    upload_to_s3(receipts, receipts_file)
    print("Datos de recibos generados y subidos a S3 correctamente.")

if __name__ == "__main__":
    main()
