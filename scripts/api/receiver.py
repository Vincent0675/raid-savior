"""
Flask HTTP receiver for WoW raid telemetry events.
Validates incoming events with Pydantic and persists to MinIO Bronze.

Phase: 2 (HTTP Receiver + Bronze Persistence)
Endpoint: POST /events
"""

from flask import Flask, request, jsonify, Response, stream_with_context, current_app
from pydantic import ValidationError
from datetime import datetime, timezone
import uuid
import json
import time

from src.schemas.eventos_schema import WoWRaidEvent
from src.storage.minio_client import MinIOStorageClient
from src.api.sse_bus import sse_bus

app = Flask(__name__)


@app.route("/health", methods=["GET"])
def health_check():
    return jsonify(
        {
            "status": "healthy",
            "service": "wow-telemetry-receiver",
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
    ), 200


@app.route("/events", methods=["POST"])
def receive_events():
    if not request.is_json:
        return jsonify({"error": "Content-Type must be application/json"}), 400

    payload = request.get_json()

    if not isinstance(payload, list):
        return jsonify({"error": "Payload must be JSON array of events"}), 400

    if len(payload) == 0:
        return jsonify({"error": "Empty payload"}), 400

    validated_events = []
    errors = []

    for idx, event_data in enumerate(payload):
        try:
            event = WoWRaidEvent(**event_data)
            validated_events.append(event)
        except ValidationError as e:
            errors.append(
                {"index": idx, "event_data": event_data, "errors": e.errors()}
            )

    if errors:
        return jsonify(
            {
                "status": "validation_failed",
                "valid_count": len(validated_events),
                "invalid_count": len(errors),
                "errors": errors[:5],
            }
        ), 400

    for ev in validated_events:
        sse_bus.publish(ev.model_dump(mode="json"))

    raid_id = validated_events[0].raid_id
    batch_id = str(uuid.uuid4())
    ingest_timestamp = datetime.now(timezone.utc).isoformat()

    batch_data = {
        "batch_id": batch_id,
        "ingest_timestamp": ingest_timestamp,
        "event_count": len(validated_events),
        "events": [event.model_dump(mode="json") for event in validated_events],
    }

    # ← Ahora lo obtiene del contexto de la app, no de un global
    storage_client = current_app.config["storage_client"]

    try:
        storage_result = storage_client.save_batch(raid_id, batch_data)
        return jsonify(
            {
                "status": "accepted",
                "batch_id": batch_id,
                "events_received": len(validated_events),
                "storage": storage_result,
                "timestamp": ingest_timestamp,
            }
        ), 201

    except Exception as e:
        return jsonify(
            {
                "status": "storage_error",
                "error": str(e),
                "events_validated": len(validated_events),
            }
        ), 500


@app.route("/stream/events", methods=["GET"])
def stream_events():
    def event_stream():
        q = sse_bus.subscribe()
        try:
            while True:
                if not q:
                    time.sleep(0.1)
                    continue
                event = q.popleft()
                yield f"data: {json.dumps(event, ensure_ascii=False)}\n\n"
        finally:
            sse_bus.unsubscribe(q)

    resp = Response(
        stream_with_context(event_stream()),
        mimetype="text/event-stream",
    )
    resp.headers["Access-Control-Allow-Origin"] = "*"
    return resp


def create_app(storage=None):
    """Application factory con DI. Si no se inyecta storage, crea uno real."""
    if storage is None:
        storage = MinIOStorageClient()  # ← Instancia real solo en producción
    app.config["storage_client"] = storage
    return app


if __name__ == "__main__":
    print("=" * 70)
    print("WoW Telemetry Receiver - Flask HTTP Server")
    print("=" * 70)
    print()
    print("Endpoints:")
    print("  GET  /health  - Health check")
    print("  POST /events  - Receive events (JSON array)")
    print()
    print("Storage: MinIO Bronze Layer")
    print("Starting server on http://localhost:5000")
    print("=" * 70)
    app_instance = create_app()
    app_instance.run(host="0.0.0.0", port=5000, debug=True)
