import requests
import uuid
from datetime import datetime, timezone

# Configuración
URL = "http://localhost:5000/events"

def generate_dummy_event():
    """Genera un evento válido ajustado a src/schemas/eventos_schema.py"""
    return {
        "event_id": str(uuid.uuid4()),
        "event_type": "combat_damage",  # Enum exacto
        "timestamp": datetime.now(timezone.utc).isoformat(),
        
        "raid_id": "raid001",           # snake_case
        "encounter_id": "enc_test_01",
        
        "source_player_id": "player_tester",
        "source_player_name": "Tester",
        "source_player_role": "dps",    # Enum exacto
        "source_player_class": "mage",  # Enum exacto
        "source_player_level": 90,
        
        "target_entity_id": "target_dummy",
        "target_entity_name": "Target Dummy",
        "target_entity_type": "interactive", # Enum exacto
        
        "damage_amount": 1500.0,        # Requerido para combat_damage
        "is_critical_hit": False,
        
        "ability_id": "123",
        "ability_name": "Fireball",
        "ability_school": "fire",
        
        "source_system": "script_manual_corrected"
    }



def run_test():
    # 1. Preparar Payload (Batch)
    event = generate_dummy_event()
    payload = [event]
    print(f"📡 Enviando evento a {URL}...")
    # print(json.dumps(payload, indent=2)) # Descomentar para debug
    
    try:
        # 2. Enviar POST
        response = requests.post(
            URL,
            json=payload,
            headers={"Content-Type": "application/json"},
            timeout=10,
            )
        
        # 3. Analizar Respuesta
        if response.status_code == 201:
            data = response.json()
            print("\n✅ ÉXITO: Batch aceptado y persistido.")
            print(f"   Batch ID: {data.get('batch_id')}")
            print(f"   Ruta S3:  {data.get('location')}")
            print("\n👉 AHORA: Ve a la consola de MinIO (http://localhost:9001) y verifica el bucket 'bronze'.")
        else:
            print(f"\n❌ ERROR {response.status_code}: El servidor rechazó el evento.")
            print(f"   Respuesta: {response.text}")
            
    except Exception as e:
        print("\n🔥 EXCEPCIÓN: No se pudo conectar al servidor. ¿Está corriendo?")
        print(e)

if __name__ == "__main__":
    run_test()
