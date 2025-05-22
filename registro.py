import json
import pika
import pymongo

# Conexión a MongoDB
mongo_client = pymongo.MongoClient("mongodb://localhost:27017/")
db = mongo_client["emergenciasDB"]
emergencias_col = db["emergencias"]

# Conexión a RabbitMQ
connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
channel = connection.channel()

channel.queue_declare(queue='registro_emergencias')

print("Servicio de registro esperando mensajes...")

def callback(ch, method, properties, body):
    try:
        data = json.loads(body)
        name = data.get("name")
        status = data.get("status")

        if status == "En curso":
            # Insertar nueva emergencia
            emergencia = {
                "emergency_id": data.get("emergency_id"),
                "name": name,
                "latitude": data.get("latitude"),
                "longitude": data.get("longitude"),
                "magnitude": data.get("magnitude"),
                "status": "En curso"
            }
            emergencias_col.insert_one(emergencia)
            print(f"Registrada nueva emergencia: {name}")

        elif status == "Extinguido":
            # Actualizar estado de emergencia
            result = emergencias_col.update_one(
                {"name": name, "status": "En curso"},
                {"$set": {"status": "Extinguido"}}
            )
            if result.modified_count > 0:
                print(f"Emergencia {name} marcada como extinguida.")
            else:
                print(f"No se encontró emergencia activa con nombre {name} para actualizar.")

    except Exception as e:
        print(f"Error procesando mensaje: {e}")

channel.basic_consume(queue='registro_emergencias', on_message_callback=callback, auto_ack=True)

channel.start_consuming()
