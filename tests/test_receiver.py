"""
Integration tests for Flask receiver.

Phase: 2.1 (HTTP Receiver - Basic)
"""

import pytest
import json
from pathlib import Path

from src.api.receiver import create_app


@pytest.fixture
def client():
    """Flask test client."""
    app = create_app()
    app.config['TESTING'] = True
    with app.test_client() as client:
        yield client


@pytest.fixture
def sample_events():
    """Load sample events from generated dataset."""
    dataset_path = Path("data/bronze/datos_generados.json")
    
    if not dataset_path.exists():
        pytest.skip("Dataset not found. Run scripts/generate_dataset.py first.")
    
    with open(dataset_path, 'r') as f:
        events = json.load(f)
    
    return events[:10]  # Use first 10 events


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
    
    assert response.status_code == 200
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
