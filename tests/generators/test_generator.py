from src.generators.class_profiles import SPEC_PROFILES, SPECS_BY_ROLE
from src.schemas.eventos_schema import EventType

def test_player_roles_coherent_with_spec_profiles(session):
    """Todos los jugadores tienen role coherente con su spec en SPEC_PROFILES."""
    from src.generators.class_profiles import SPEC_PROFILES
    for p in session.players:
        expected = SPEC_PROFILES[(p.player_class, p.spec)]["role"]
        assert p.role == expected, \
            f"❌ {p.player_class}/{p.spec}: role='{p.role}', esperado='{expected}'"
        
def test_get_ability_damage_in_spec_catalog(generator, session):
    dps = next(p for p in session.players if p.role == "dps")
    ability = generator._get_ability(dps, "damage")
    
    # Assert: la ability devuelta debe estar en el catálogo de su spec
    valid_names = [
        a["ability_name"]
        for a in SPEC_PROFILES[(dps.player_class, dps.spec)]["abilities"]["damage"]
    ]
    assert ability["ability_name"] in valid_names

def test_get_ability_heal_in_spec_catalog(generator, session):
    heal = next(p for p in session.players if p.role == "healer")
    ability = generator._get_ability(heal, "heal")
    
    # Assert: la ability devuelta debe estar en el catálogo de su spec
    valid_names = [
        a["ability_name"]
        for a in SPEC_PROFILES[(heal.player_class, heal.spec)]["abilities"]["heal"]
    ]
    assert ability["ability_name"] in valid_names


def test_get_ability_fallback_for_empty_list(generator, session):
    # Un DPS puro no tiene heal abilities → debe devolver el fallback
    dps = next(p for p in session.players if p.role == "dps"
               and SPEC_PROFILES[(p.player_class, p.spec)]["abilities"]["heal"] == [])
    
    ability = generator._get_ability(dps, "heal")
    
    assert ability["ability_name"] == "Auto Attack"
    assert ability["ability_school"] == "physical"

def test_create_damage_event_type_and_ability(generator, session, dummy_phase, past_timestamp):
    dps = next(p for p in session.players if p.role == "dps")
    ev  = generator._create_damage_event(dps, session, past_timestamp, dummy_phase)

    assert ev.event_type == EventType.COMBAT_DAMAGE
    assert ev.damage_amount is not None and ev.damage_amount > 0

    valid_names = [
        a["ability_name"]
        for a in SPEC_PROFILES[(dps.player_class, dps.spec)]["abilities"]["damage"]
    ]
    assert ev.ability_name in valid_names

def test_create_heal_event_type_and_ability(generator, session, dummy_phase, past_timestamp):
    healer = next(p for p in session.players if p.role == "healer")
    ev  = generator._create_heal_event(healer, session, past_timestamp, dummy_phase)

    assert ev.event_type == EventType.HEAL
    assert ev.healing_amount is not None and ev.healing_amount > 0

    valid_names = [
        a["ability_name"]
        for a in SPEC_PROFILES[(healer.player_class, healer.spec)]["abilities"]["heal"]
    ]
    assert ev.ability_name in valid_names

def test_create_spell_cast_healer_uses_heal_ability(generator, session, dummy_phase, past_timestamp):
    healer = next(p for p in session.players if p.role == "healer")
    ev     = generator._create_spell_cast_event(healer, session, past_timestamp, dummy_phase)

    from src.schemas.eventos_schema import EventType
    assert ev.event_type == EventType.SPELL_CAST

    # Un healer debe lanzar una heal ability, no una de daño
    valid_heal_names = [
        a["ability_name"]
        for a in SPEC_PROFILES[(healer.player_class, healer.spec)]["abilities"]["heal"]
    ]
    assert ev.ability_name in valid_heal_names, \
        f"Healer {healer.player_class}/{healer.spec} lanzó '{ev.ability_name}' que no es heal"

def test_mana_regen_resource_matches_spec(generator, session, past_timestamp):
    for role in ["dps", "healer", "tank"]:
        p  = next(pl for pl in session.players if pl.role == role)
        ev = generator._create_mana_regen_event(p, session, past_timestamp)

        expected_resource = p.resource_type          # string: "mana", "rage", etc.
        assert ev.resource_type.value == expected_resource, \
            f"{p.player_class}/{p.spec}: esperado '{expected_resource}', tiene '{ev.resource_type.value}'"

def test_generate_events_count_and_ordering(events):
    assert len(events) >= 1000   # ≥ porque boss_phase events se añaden extra

    for i in range(len(events) - 1):
        assert events[i].timestamp <= events[i + 1].timestamp, \
            f"Timestamp desordenado en índice {i}"

def test_generate_events_unique_ids(events):
    ids = [e.event_id for e in events]
    assert len(set(ids)) == len(ids), \
        f"IDs duplicados: {len(ids) - len(set(ids))} duplicados encontrados"

def test_damage_events_no_pure_healer_as_source(session, events):
    damage_events = [e for e in events if e.event_type.value == "combat_damage"]
    
    # Construir lookup rápido player_id → player
    player_map = {p.player_id: p for p in session.players}
    
    for ev in damage_events:
        # boss_phase events tienen como source el boss_id, no un player_id
        if ev.source_player_id not in player_map:
            continue
        
        source = player_map[ev.source_player_id]
        profile = SPEC_PROFILES[(source.player_class, source.spec)]
        
        # Un healer puro tiene combat_damage weight < 0.10
        # Si genera combat_damage, debe tener al menos alguna damage ability
        if profile["event_weights"]["combat_damage"] < 0.10:
            assert profile["abilities"]["damage"] != [], \
                f"Healer puro {source.player_class}/{source.spec} generó combat_damage sin damage abilities"
