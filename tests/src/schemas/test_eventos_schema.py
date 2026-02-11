"""
Tests de validación para eventos_schema.py

Verifica que:
1. Eventos válidos pasen validación
2. Eventos inválidos sean rechazados con errores claros
3. Validadores personalizados funcionen correctamente
"""

import pytest
from datetime import datetime, timezone, timedelta
from uuid import uuid4
from pydantic import ValidationError

from src.schemas.eventos_schema import (
    WoWRaidEvent,
    EventType,
    PlayerRole,
    PlayerClass,
    DamageSchool,
    EntityType,
    ResourceType
)


def test_valid_combat_damage_event():
    """Test: Evento de daño válido debe pasar validación."""
    event = WoWRaidEvent(
        event_type=EventType.COMBAT_DAMAGE,
        timestamp=datetime.now(timezone.utc),
        raid_id="raid001",
        source_player_id="player123",
        source_player_name="Thrall",
        source_player_role=PlayerRole.DPS,
        source_player_class=PlayerClass.SHAMAN,
        source_player_level=70,
        target_entity_id="boss001",
        target_entity_name="Ragnaros",
        target_entity_type=EntityType.BOSS,
        ability_name="Pyroblast",
        ability_school=DamageSchool.FIRE,
        damage_amount=15000.0,
        is_critical_hit=True,
        critical_multiplier=1.5
    )
    
    assert event.event_type == EventType.COMBAT_DAMAGE
    assert event.damage_amount == 15000.0
    assert event.is_critical_hit is True
    print("✅ Test passed: Valid combat_damage event")


def test_invalid_damage_event_without_damage():
    """Test: Evento de daño SIN damage_amount debe ser rechazado."""
    with pytest.raises(ValidationError) as exc_info:
        WoWRaidEvent(
            event_type=EventType.COMBAT_DAMAGE,
            timestamp=datetime.now(timezone.utc),
            raid_id="raid001",
            source_player_id="player123",
            source_player_name="Thrall",
            # damage_amount falta! ❌
        )
    
    errors = exc_info.value.errors()
    assert any("damage_amount" in str(e) for e in errors)
    print("✅ Test passed: combat_damage without damage rejected")


def test_timestamp_in_future_rejected():
    """Test: Timestamp en el futuro debe ser rechazado."""
    future_time = datetime.now(timezone.utc) + timedelta(hours=1)
    
    with pytest.raises(ValidationError) as exc_info:
        WoWRaidEvent(
            event_type=EventType.HEAL,
            timestamp=future_time,  # ❌ Futuro
            raid_id="raid001",
            source_player_id="player456",
            source_player_name="Morissa",
            healing_amount=8500.0
        )
    
    errors = exc_info.value.errors()
    assert any("future" in str(e).lower() for e in errors)
    print("✅ Test passed: Future timestamp rejected")


def test_valid_heal_event():
    """Test: Evento de curación válido debe pasar."""
    event = WoWRaidEvent(
        event_type=EventType.HEAL,
        timestamp=datetime.now(timezone.utc),
        raid_id="raid002",
        source_player_id="player456",
        source_player_name="Morissa",
        source_player_role=PlayerRole.HEALER,
        source_player_class=PlayerClass.PRIEST,
        target_entity_id="player123",
        target_entity_name="Thrall",
        target_entity_type=EntityType.PLAYER,
        ability_name="Healing Touch",
        ability_school=DamageSchool.HOLY,
        healing_amount=8500.0,
        is_critical_hit=False
    )
    
    assert event.healing_amount == 8500.0
    print("✅ Test passed: Valid heal event")


def test_invalid_raid_id_format():
    """Test: raid_id con formato inválido debe ser rechazado."""
    with pytest.raises(ValidationError) as exc_info:
        WoWRaidEvent(
            event_type=EventType.SPELL_CAST,
            timestamp=datetime.now(timezone.utc),
            raid_id="invalid_raid",  # ❌ No cumple patrón raid\d{3}
            source_player_id="player789",
            source_player_name="Uther"
        )
    
    errors = exc_info.value.errors()
    assert any("raid_id" in str(e) or "pattern" in str(e) for e in errors)
    print("✅ Test passed: Invalid raid_id format rejected")


def test_valid_mana_regeneration_event():
    """Test: Evento de regeneración de maná válido."""
    event = WoWRaidEvent(
        event_type=EventType.MANA_REGENERATION,
        timestamp=datetime.now(timezone.utc),
        raid_id="raid003",
        source_player_id="player456",
        source_player_name="Morissa",
        source_player_role=PlayerRole.HEALER,
        resource_type=ResourceType.MANA,
        resource_amount_before=5000.0,
        resource_amount_after=6000.0,
        resource_regeneration_rate=50.0
    )
    
    assert event.resource_type == ResourceType.MANA
    assert event.resource_amount_after > event.resource_amount_before
    print("✅ Test passed: Valid mana_regeneration event")


def test_extra_fields_rejected():
    """Test: Campos extra deben ser rechazados (extra='forbid')."""
    with pytest.raises(ValidationError) as exc_info:
        WoWRaidEvent(
            event_type=EventType.SPELL_CAST,
            timestamp=datetime.now(timezone.utc),
            raid_id="raid001",
            source_player_id="player123",
            source_player_name="Thrall",
            invalid_extra_field="should_fail"  # ❌ Campo no definido
        )
    
    errors = exc_info.value.errors()
    assert any("extra" in str(e).lower() or "forbidden" in str(e).lower() for e in errors)
    print("✅ Test passed: Extra fields rejected")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
