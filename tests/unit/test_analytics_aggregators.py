import pandas as pd

from src.analytics.aggregators import build_raid_summary, build_player_raid_stats

def test_build_raid_summary_simple_success():
    # 1. DataFrame Silver mínimo con una sola raid
    data = [
        # Evento de daño al boss (kill)
        {
            "raid_id": "raid001",
            "timestamp": pd.Timestamp("2026-01-01T10:00:00Z"),
            "event_date": "2026-01-01",
            "event_type": "combat_damage",
            "damage_amount": 15000.0,
            "healing_amount": 0.0,
            "target_entity_type": "boss",
            "target_entity_health_pct_after": 0.0,
            "source_player_id": "player1",
            "source_player_role": "dps",
        },
        # Evento de curación de un healer
        {
            "raid_id": "raid001",
            "timestamp": pd.Timestamp("2026-01-01T10:00:01Z"),
            "event_date": "2026-01-01",
            "event_type": "heal",
            "damage_amount": 0.0,
            "healing_amount": 5000.0,
            "target_entity_type": "player",
            "target_entity_health_pct_after": 100.0,
            "source_player_id": "player2",
            "source_player_role": "healer",
        },
        # Evento de muerte del healer
        {
            "raid_id": "raid001",
            "timestamp": pd.Timestamp("2026-01-01T10:00:02Z"),
            "event_date": "2026-01-01",
            "event_type": "player_death",
            "damage_amount": 0.0,
            "healing_amount": 0.0,
            "target_entity_type": "player",
            "target_entity_health_pct_after": 0.0,
            "source_player_id": "player2",
            "source_player_role": "healer",
        },
    ]

    df_silver = pd.DataFrame(data)

    # 2. Ejecutar la agregación Gold
    raid_summary = build_raid_summary(df_silver)
    print("Columnas devueltas:", raid_summary.columns.tolist())
    print("DataFrame completo:")
    print(raid_summary)

    # 3. Comprobaciones básicas
    assert len(raid_summary) == 1

    row = raid_summary.iloc[0]

    # Identidad y fecha
    assert row["raid_id"] == "raid001"
    assert row["event_date"] == "2026-01-01"

    # Agregados numéricos
    assert row["total_damage"] == 15000.0
    assert row["total_healing"] == 5000.0
    assert row["total_player_deaths"] == 1
    assert row["n_players"] == 2
    assert row["n_tanks"] == 0
    assert row["n_healers"] == 1
    assert row["n_dps"] == 1

    # Métricas derivadas
    assert row["boss_min_hp_pct"] == 0.0
    assert row["raid_outcome"] == "success"



def test_build_raid_summary_wipe_case():
    # 1. DataFrame Silver mínimo para un caso de wipe
    data = [
        # Daño al boss, pero no lo matan (se queda al 50 %)
        {
            "raid_id": "raid002",
            "timestamp": pd.Timestamp("2026-01-02T20:00:00Z"),
            "event_date": "2026-01-02",
            "event_type": "combat_damage",
            "damage_amount": 8000.0,
            "healing_amount": 0.0,
            "target_entity_type": "boss",
            "target_entity_health_pct_after": 50.0,
            "source_player_id": "playerA",
            "source_player_role": "dps",
        },
        # Algo de curación
        {
            "raid_id": "raid002",
            "timestamp": pd.Timestamp("2026-01-02T20:00:05Z"),
            "event_date": "2026-01-02",
            "event_type": "heal",
            "damage_amount": 0.0,
            "healing_amount": 3000.0,
            "target_entity_type": "player",
            "target_entity_health_pct_after": 100.0,
            "source_player_id": "playerB",
            "source_player_role": "healer",
        },
        # Varias muertes (más que n_players = 2)
        {
            "raid_id": "raid002",
            "timestamp": pd.Timestamp("2026-01-02T20:00:10Z"),
            "event_date": "2026-01-02",
            "event_type": "player_death",
            "damage_amount": 0.0,
            "healing_amount": 0.0,
            "target_entity_type": "player",
            "target_entity_health_pct_after": 0.0,
            "source_player_id": "playerA",
            "source_player_role": "dps",
        },
        {
            "raid_id": "raid002",
            "timestamp": pd.Timestamp("2026-01-02T20:00:12Z"),
            "event_date": "2026-01-02",
            "event_type": "player_death",
            "damage_amount": 0.0,
            "healing_amount": 0.0,
            "target_entity_type": "player",
            "target_entity_health_pct_after": 0.0,
            "source_player_id": "playerB",
            "source_player_role": "healer",
        },
        {
            "raid_id": "raid002",
            "timestamp": pd.Timestamp("2026-01-02T20:00:14Z"),
            "event_date": "2026-01-02",
            "event_type": "player_death",
            "damage_amount": 0.0,
            "healing_amount": 0.0,
            "target_entity_type": "player",
            "target_entity_health_pct_after": 0.0,
            "source_player_id": "playerA",
            "source_player_role": "dps",
        },
        {
            "raid_id": "raid002",
            "timestamp": pd.Timestamp("2026-01-02T20:00:16Z"),
            "event_date": "2026-01-02",
            "event_type": "player_death",
            "damage_amount": 0.0,
            "healing_amount": 0.0,
            "target_entity_type": "player",
            "target_entity_health_pct_after": 0.0,
            "source_player_id": "playerB",
            "source_player_role": "healer",
        },
    ]

    df_silver = pd.DataFrame(data)

    # 2. Ejecutar la agregación Gold
    raid_summary = build_raid_summary(df_silver)

    # 3. Comprobaciones
    assert len(raid_summary) == 1
    row = raid_summary.iloc[0]

    # Identidad básica
    assert row["raid_id"] == "raid002"
    assert row["event_date"] == "2026-01-02"

    # Comprobar que se han agregado bien las métricas clave
    assert row["total_damage"] == 8000.0
    assert row["total_healing"] == 3000.0
    assert row["total_player_deaths"] == 4
    assert row["n_players"] == 2

    # Composición de roles
    assert row["n_tanks"] == 0
    assert row["n_healers"] == 1
    assert row["n_dps"] == 1


    # boss_min_hp_pct alto y resultado wipe
    assert row["boss_min_hp_pct"] == 50.0
    assert row["raid_outcome"] == "wipe"

def test_build_player_raid_stats_simple():
    # Mismo Silver que en el test success anterior
    data = [
        {
            "raid_id": "raid001",
            "timestamp": pd.Timestamp("2026-01-01T10:00:00Z"),
            "event_date": "2026-01-01",
            "event_type": "combat_damage",
            "damage_amount": 15000.0,
            "healing_amount": 0.0,
            "target_entity_type": "boss",
            "target_entity_id": "player1",
            "target_entity_health_pct_after": 0.0,
            "source_player_id": "player1",
            "source_player_name": "Alice",
            "source_player_class": "mage",
            "source_player_role": "dps",
            "is_critical_hit": True,
        },
        {
            "raid_id": "raid001",
            "timestamp": pd.Timestamp("2026-01-01T10:00:01Z"),
            "event_date": "2026-01-01",
            "event_type": "heal",
            "damage_amount": 0.0,
            "healing_amount": 5000.0,
            "target_entity_type": "player",
            "target_entity_id": "player2",
            "target_entity_health_pct_after": 100.0,
            "source_player_id": "player2",
            "source_player_name": "Bob",
            "source_player_class": "priest",
            "source_player_role": "healer",
            "is_critical_hit": False,
        },
        {
            "raid_id": "raid001",
            "timestamp": pd.Timestamp("2026-01-01T10:00:02Z"),
            "event_date": "2026-01-01",
            "event_type": "player_death",
            "damage_amount": 0.0,
            "healing_amount": 0.0,
            "target_entity_type": "player",
            "target_entity_id": "player1",
            "target_entity_health_pct_after": 0.0,
            "source_player_id": "player2",
            "source_player_name": "Bob",
            "source_player_class": "priest",
            "source_player_role": "healer",
            "is_critical_hit": False,
        },
    ]
    
    df_silver = pd.DataFrame(data)
    
    # Primero construir raid_summary
    raid_summary = build_raid_summary(df_silver)
    
    # Ahora player_raid_stats
    player_stats = build_player_raid_stats(df_silver, raid_summary)
    
    # Comprobaciones
    assert len(player_stats) == 2  # 2 jugadores
    
    # Jugador 1 (Alice, dps)
    alice = player_stats[player_stats["player_id"] == "player1"].iloc[0]
    assert alice["player_name"] == "Alice"
    assert alice["player_role"] == "dps"
    assert alice["damage_total"] == 15000.0
    assert alice["healing_total"] == 0.0
    assert alice["player_deaths"] == 0
    assert alice["dps"] > 0  # algún valor positivo
    
    # Jugador 2 (Bob, healer)
    bob = player_stats[player_stats["player_id"] == "player2"].iloc[0]
    assert bob["player_name"] == "Bob"
    assert bob["player_role"] == "healer"
    assert bob["damage_total"] == 0.0
    assert bob["healing_total"] == 5000.0
    assert bob["player_deaths"] == 1
    assert bob["hps"] > 0

    # Verificar conteos y métricas adicionales
    assert alice["damage_events"] == 1
    assert alice["crit_events"] == 1
    assert alice["crit_rate"] == 1.0  # 100% de críticos (1 de 1)
    assert alice["damage_share"] == 1.0  # es el único que hace daño

    assert bob["healing_events"] == 1
    assert bob["crit_events"] == 0
    assert bob["crit_rate"] == 0.0
    assert bob["healing_share"] == 1.0  # es el único que cura