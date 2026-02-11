import sys
import os
import uuid
from datetime import datetime, timezone
from flask import Flask, request, jsonify
from pydantic import ValidationError

# Añadimos el directorio raíz al path para poder importar módulos hermanos
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from src.config import Config
from src.storage.minio_client import MinIOStorageClient
from src.schemas.eventos_schema import WoWRaidEvent
from src.api.sse_bus import sse_bus

app = Flask(__name__)
storage_client = MinIOStorageClient()

@app.route('/health', methods=['GET'])
def health_check():
    """Endpoint para verificar que el servicio está vivo (Heartbeat)."""
    return jsonify({"status": "healthy", "service": "wow-telemetry-receiver"}), 200

@app.route('/events', methods=['POST'])
def ingest_events():
    """
    Ingesta de Batch de Eventos.
    Aplica Schema-on-Write: Valida antes de guardar.
    """
    # 1. Parseo básico del JSON
    try:
        payload = request.get_json()
        if not payload:
            return jsonify({"error": "Empty payload"}), 400
    except Exception as e:
        return jsonify({"error": f"Invalid JSON format: {str(e)}"}), 400

    # Extraemos la lista de eventos
    # Soportamos tanto una lista directa [...] como un objeto {"events": [...]}
    raw_events = payload.get('events', []) if isinstance(payload, dict) else payload
    
    if not isinstance(raw_events, list):
        return jsonify({"error": "Payload must contain a list of events"}), 400

    valid_events = []
    errors = []

    # 2. Validación estricta con Pydantic
    for index, event_data in enumerate(raw_events):
        try:
            # Aquí ocurre la magia: Pydantic valida tipos, rangos y enums
            validated_event = WoWRaidEvent(**event_data)
            valid_events.append(validated_event.model_dump(mode='json'))
        except ValidationError as e:
            # Si falla, capturamos el error específico
            errors.append({
                "index": index,
                "error": e.errors()
            })

    # Decisión de Diseño: ¿Rechazamos todo el batch si hay un error?
    # Para sistemas estrictos (Fase 2), SÍ. Todo o nada (Atomicidad del batch).
    if errors:
        return jsonify({
            "status": "rejected",
            "message": "Schema validation failed",
            "errors_count": len(errors),
            "details": errors
        }), 400

    if not valid_events:
        return jsonify({"message": "No events provided"}), 400

    # 3. Preparar Batch para Persistencia
    # Asumimos que todos los eventos del batch son de la misma Raid (por simplicidad ahora)
    # En producción real, agruparíamos por RaidID.
    raid_id = valid_events[0].get('raid_id', 'unknown_raid')
    batch_id = str(uuid.uuid4())
    ingest_ts = datetime.now(timezone.utc).isoformat()

    batch_container = {
        "batch_id": batch_id,
        "ingest_timestamp": ingest_ts,
        "event_count": len(valid_events),
        "events": valid_events
    }


    # 4. Guardar en MinIO (Bronze Layer)
    try:
        result = storage_client.save_batch(raid_id, batch_container)
        return jsonify({
            "status": "success", 
            "batch_id": batch_id,
            "location": result['s3_path']
        }), 201
    except Exception as e:
        return jsonify({"error": f"Storage failure: {str(e)}"}), 500

@app.route("/stream/events", methods=["GET"])
def stream_events():
    """
    Endpoint SSE que emite todos los eventos validados en tiempo real.
    """

    def event_stream():
        # Cada cliente obtiene su propia cola
        q = sse_bus.subscribe()
        try:
            while True:
                if not q:
                    # No hay eventos pendientes: dormimos un poco para no quemar CPU
                    time.sleep(0.1)
                    continue

                event = q.popleft()
                # Formato SSE: data: <json>\n\n
                yield f"data: {json.dumps(event, ensure_ascii=False)}\n\n"
        finally:
            # Limpieza cuando el cliente cierra conexión
            sse_bus.unsubscribe(q)

    return Response(
        stream_with_context(event_stream()),
        mimetype="text/event-stream",
    )

if __name__ == '__main__':
    # Ejecución local para desarrollo
    app.run(host=Config.FLASK_HOST, port=Config.FLASK_PORT, debug=True)
