"""
Flask HTTP receiver for WoW raid telemetry events.
Validates incoming events with Pydantic before acknowledging.

Phase: 2.1 (HTTP Receiver - Basic)
Endpoint: POST /events
"""

from flask import Flask, request, jsonify
from pydantic import ValidationError
from typing import List, Dict, Any
from datetime import datetime

from src.schemas.eventos_schema import WoWRaidEvent


app = Flask(__name__)


@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint for monitoring."""
    return jsonify({
        "status": "healthy",
        "service": "wow-telemetry-receiver",
        "timestamp": datetime.utcnow().isoformat()
    }), 200


@app.route('/events', methods=['POST'])
def receive_events():
    """
    Receive and validate WoW raid events.
    
    Expects JSON array of events.
    Returns 200 if all valid, 400 if any validation fails.
    """
    if not request.is_json:
        return jsonify({
            "error": "Content-Type must be application/json"
        }), 400
    
    payload = request.get_json()
    
    # Ensure payload is a list
    if not isinstance(payload, list):
        return jsonify({
            "error": "Payload must be JSON array of events"
        }), 400
    
    if len(payload) == 0:
        return jsonify({
            "error": "Empty payload"
        }), 400
    
    # Validate each event with Pydantic
    validated_events = []
    errors = []
    
    for idx, event_data in enumerate(payload):
        try:
            event = WoWRaidEvent(**event_data)
            validated_events.append(event)
        except ValidationError as e:
            errors.append({
                "index": idx,
                "event_data": event_data,
                "errors": e.errors()
            })
    
    # If any validation failed, reject entire batch
    if errors:
        return jsonify({
            "status": "validation_failed",
            "valid_count": len(validated_events),
            "invalid_count": len(errors),
            "errors": errors[:5]  # Return first 5 errors
        }), 400
    
    # All events validated successfully
    return jsonify({
        "status": "accepted",
        "events_received": len(validated_events),
        "timestamp": datetime.utcnow().isoformat()
    }), 200


def create_app():
    """Application factory pattern for testing."""
    return app


if __name__ == '__main__':
    print("=" * 70)
    print("WoW Telemetry Receiver - Flask HTTP Server")
    print("=" * 70)
    print()
    print("Endpoints:")
    print("  GET  /health  - Health check")
    print("  POST /events  - Receive events (JSON array)")
    print()
    print("Starting server on http://localhost:5000")
    print("=" * 70)
    
    app.run(host='0.0.0.0', port=5000, debug=True)
