"""
Integration tests for Flask receiver.

Phase: 2.1 (HTTP Receiver - Basic)
"""

import pytest
import json
from pathlib import Path

from src.api.receiver import create_app
from src.generators.raid_event_generator import WoWEventGenerator


@pytest.fixture
def client():
    """Flask test client."""
    app = create_app()
    app.config['TESTING'] = True
    with app.test_client() as client:
        yield client


@pytest.fixture
def sample_events():
    generator = WoWEventGenerator(seed=42)
    session = generator.generate_raid_session(
        raid_id="raid999",
        num_players=5,
        duration_s=30,
    )
    events = generator.generate_events(session, num_events=10)
    return [e.model_dump(mode="json") for e in events]


def test_health_check(client):
    """Test health endpoint."""
    response = client.get('/health')
    
    assert response.status_code == 200
    data = response.get_json()
    assert data['status'] == 'healthy'
    assert 'timestamp' in data


def test_post_valid_events(client, sample_events):
    """Test POST with valid events."""
    response = client.post(
        '/events',
        data=json.dumps(sample_events),
        content_type='application/json'
    )
    
    assert response.status_code == 201
    data = response.get_json()
    assert data['status'] == 'accepted'
    assert data['events_received'] == len(sample_events)


def test_post_invalid_event(client):
    """Test POST with invalid event (missing required field)."""
    invalid_payload = [
        {
            "event_id": "550e8400-e29b-41d4-a716-446655440000",
            # Missing required fields: timestamp, raid_id, etc.
        }
    ]
    
    response = client.post(
        '/events',
        data=json.dumps(invalid_payload),
        content_type='application/json'
    )
    
    assert response.status_code == 400
    data = response.get_json()
    assert data['status'] == 'validation_failed'
    assert data['invalid_count'] == 1


def test_post_empty_payload(client):
    """Test POST with empty array."""
    response = client.post(
        '/events',
        data=json.dumps([]),
        content_type='application/json'
    )
    
    assert response.status_code == 400
    data = response.get_json()
    assert 'error' in data


def test_post_non_json(client):
    """Test POST with non-JSON content."""
    response = client.post(
        '/events',
        data="not json",
        content_type='text/plain'
    )
    
    assert response.status_code == 400
    data = response.get_json()
    assert 'error' in data
