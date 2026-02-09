import pandas as pd

from src.analytics.aggregators import build_raid_summary, build_player_raid_stats

def test_build_raid_summary_simple_success():
    # 1. DataFrame Silver mínimo con una sola raid
    data = [
        # Evento de daño al boss (kill)
        {
            "raidid": "raid001",
            "timestamp": pd.Timestamp("2026-01-01T10:00:00Z"),
            "eventdate": "2026-01-01",
            "eventtype": "combatdamage",
            "damageamount": 15000.0,
            "healingamount": 0.0,
            "targetentitytype": "boss",
            "targetentityhealthpctafter": 0.0,
            "sourceplayerid": "player1",
            "sourceplayerrole": "dps",
        },
        # Evento de curación de un healer
        {
            "raidid": "raid001",
            "timestamp": pd.Timestamp("2026-01-01T10:00:01Z"),
            "eventdate": "2026-01-01",
            "eventtype": "heal",
            "damageamount": 0.0,
            "healingamount": 5000.0,
            "targetentitytype": "player",
            "targetentityhealthpctafter": 100.0,
            "sourceplayerid": "player2",
            "sourceplayerrole": "healer",
        },
        # Evento de muerte del healer
        {
            "raidid": "raid001",
            "timestamp": pd.Timestamp("2026-01-01T10:00:02Z"),
            "eventdate": "2026-01-01",
            "eventtype": "playerdeath",
            "damageamount": 0.0,
            "healingamount": 0.0,
            "targetentitytype": "player",
            "targetentityhealthpctafter": 0.0,
            "sourceplayerid": "player2",
            "sourceplayerrole": "healer",
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
            "raidid": "raid002",
            "timestamp": pd.Timestamp("2026-01-02T20:00:00Z"),
            "eventdate": "2026-01-02",
            "eventtype": "combatdamage",
            "damageamount": 8000.0,
            "healingamount": 0.0,
            "targetentitytype": "boss",
            "targetentityhealthpctafter": 50.0,
            "sourceplayerid": "playerA",
            "sourceplayerrole": "dps",
        },
        # Algo de curación
        {
            "raidid": "raid002",
            "timestamp": pd.Timestamp("2026-01-02T20:00:05Z"),
            "eventdate": "2026-01-02",
            "eventtype": "heal",
            "damageamount": 0.0,
            "healingamount": 3000.0,
            "targetentitytype": "player",
            "targetentityhealthpctafter": 100.0,
            "sourceplayerid": "playerB",
            "sourceplayerrole": "healer",
        },
        # Varias muertes (más que n_players = 2)
        {
            "raidid": "raid002",
            "timestamp": pd.Timestamp("2026-01-02T20:00:10Z"),
            "eventdate": "2026-01-02",
            "eventtype": "playerdeath",
            "damageamount": 0.0,
            "healingamount": 0.0,
            "targetentitytype": "player",
            "targetentityhealthpctafter": 0.0,
            "sourceplayerid": "playerA",
            "sourceplayerrole": "dps",
        },
        {
            "raidid": "raid002",
            "timestamp": pd.Timestamp("2026-01-02T20:00:12Z"),
            "eventdate": "2026-01-02",
            "eventtype": "playerdeath",
            "damageamount": 0.0,
            "healingamount": 0.0,
            "targetentitytype": "player",
            "targetentityhealthpctafter": 0.0,
            "sourceplayerid": "playerB",
            "sourceplayerrole": "healer",
        },
        {
            "raidid": "raid002",
            "timestamp": pd.Timestamp("2026-01-02T20:00:14Z"),
            "eventdate": "2026-01-02",
            "eventtype": "playerdeath",
            "damageamount": 0.0,
            "healingamount": 0.0,
            "targetentitytype": "player",
            "targetentityhealthpctafter": 0.0,
            "sourceplayerid": "playerA",
            "sourceplayerrole": "dps",
        },
        {
            "raidid": "raid002",
            "timestamp": pd.Timestamp("2026-01-02T20:00:16Z"),
            "eventdate": "2026-01-02",
            "eventtype": "playerdeath",
            "damageamount": 0.0,
            "healingamount": 0.0,
            "targetentitytype": "player",
            "targetentityhealthpctafter": 0.0,
            "sourceplayerid": "playerB",
            "sourceplayerrole": "healer",
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
            "raidid": "raid001",
            "timestamp": pd.Timestamp("2026-01-01T10:00:00Z"),
            "eventdate": "2026-01-01",
            "eventtype": "combatdamage",
            "damageamount": 15000.0,
            "healingamount": 0.0,
            "targetentitytype": "boss",
            "targetentityid": "player1",
            "targetentityhealthpctafter": 0.0,
            "sourceplayerid": "player1",
            "sourceplayername": "Alice",
            "sourceplayerclass": "mage",
            "sourceplayerrole": "dps",
            "iscriticalhit": True,
        },
        {
            "raidid": "raid001",
            "timestamp": pd.Timestamp("2026-01-01T10:00:01Z"),
            "eventdate": "2026-01-01",
            "eventtype": "heal",
            "damageamount": 0.0,
            "healingamount": 5000.0,
            "targetentitytype": "player",
            "targetentityid": "player2",
            "targetentityhealthpctafter": 100.0,
            "sourceplayerid": "player2",
            "sourceplayername": "Bob",
            "sourceplayerclass": "priest",
            "sourceplayerrole": "healer",
            "iscriticalhit": False,
        },
        {
            "raidid": "raid001",
            "timestamp": pd.Timestamp("2026-01-01T10:00:02Z"),
            "eventdate": "2026-01-01",
            "eventtype": "playerdeath",
            "damageamount": 0.0,
            "healingamount": 0.0,
            "targetentitytype": "player",
            "targetentityid": "player1",
            "targetentityhealthpctafter": 0.0,
            "sourceplayerid": "player2",
            "sourceplayername": "Bob",
            "sourceplayerclass": "priest",
            "sourceplayerrole": "healer",
            "iscriticalhit": False,
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