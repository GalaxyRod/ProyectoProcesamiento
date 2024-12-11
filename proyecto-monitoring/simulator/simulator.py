import random
import time
import datetime
import json
from kafka import KafkaProducer

# Coordenadas aproximadas para Samborondón y Daule
LOCATIONS = {
    "Samborondon": [
        {"latitude": -1.954561, "longitude": -79.899508},
        {"latitude": -1.947000, "longitude": -79.910000},
        {"latitude": -1.943000, "longitude": -79.890000}
    ],
    "Daule": [
        {"latitude": -1.866858, "longitude": -79.977940},
        {"latitude": -1.872000, "longitude": -79.960000},
        {"latitude": -1.880000, "longitude": -79.950000}
    ]
}

# Configuración del productor Kafka
KAFKA_BROKER = "localhost:9092"  # Cambia esto si usas un entorno remoto o Docker Compose
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Función para generar datos simulados
def generate_data():
    while True:
        # Seleccionar aleatoriamente una región (Samborondón o Daule)
        region = random.choice(list(LOCATIONS.keys()))
        location = random.choice(LOCATIONS[region])

        # Generar datos simulados
        data = {
            "timestamp": datetime.datetime.now().isoformat(),
            "consumption_kWh": round(random.uniform(0.1, 5.0), 2),  # Consumo aleatorio entre 0.1 y 5 kWh
            "latitude": location["latitude"],
            "longitude": location["longitude"],
            "meter_id": f"meter_{random.randint(1, 1000)}",  # ID único del medidor
            "region": region
        }

        # Determinar el tópico correspondiente
        topic = f"{region.lower()}_data"

        # Enviar datos a Kafka
        producer.send(topic, value=data)
        print(f"Sent to topic {topic}: {data}")

        # Esperar 1 segundo antes de generar el próximo dato
        time.sleep(1)

# Iniciar la generación de datos
if __name__ == "__main__":
    try:
        generate_data()
    except KeyboardInterrupt:
        print("Simulador detenido.")
    except Exception as e:
        print(f"Error: {e}")
