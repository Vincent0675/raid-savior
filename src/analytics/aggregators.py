import pandas as pd
import numpy as np

def build_raid_summary(df_silver: pd.DataFrame) -> pd.DataFrame:
    """
    Construye la tabla gold.raid_summary a partir de eventos Silver.

    Parámetros
    ----------
    df_silver : pd.DataFrame
        DataFrame de eventos de la capa Silver. Debe contener, como mínimo,
        las columnas:
        - raidid, timestamp, eventdate
        - eventtype
        - damageamount, healingamount
        - targetentitytype, targetentityhealthpctafter
        - sourceplayerid, sourceplayerrole

    Devuelve
    --------
    pd.DataFrame
        DataFrame agregado con una fila por raid_id y las columnas:
        raid_id, event_date, duration_ms,
        total_damage, total_healing, total_player_deaths,
        n_players,
        raid_dps, raid_hps, boss_min_hp_pct,
        raid_outcome, encounter_id, n_tanks, n_healers,
        n_dps, max_concurrent_deaths
    """

    # 1. Copia del dataset
    df = df_silver.copy()

    # 2.- Cálculo de agregados base por raid
    # Duración y nº de jugadores
    group = df.groupby("raidid")

    duration = (
        group["timestamp"]
        .agg(lambda s: (s.max() - s.min()).total_seconds() * 1000.0)
        .rename("duration_ms")
    )

    n_players = (
        group["sourceplayerid"]
        .nunique()
        .rename("n_players")
    )

    # Contar jugadores por rol (tank, healer, dps)
    role_counts = (
        df.groupby(["raidid", "sourceplayerrole"])["sourceplayerid"]
        .nunique()
        .unstack(fill_value=0)
    )

    # Asegurarnos de que las columnas existen aunque algún rol no aparezca
    for role in ["tank", "healer", "dps"]:
        if role not in role_counts.columns:
            role_counts[role] = 0

    role_counts = role_counts.reset_index()

    # Renombrar columnas a n_tanks, n_healers, n_dps
    role_counts = role_counts.rename(
        columns={
            "raidid": "raidid",
            "tank": "n_tanks",
            "healer": "n_healers",
            "dps": "n_dps",
        }
    )

    # event_date: mínimo eventdate
    event_date = (
        group["eventdate"]
        .min()
        .rename("event_date")
    )

    raid_base = pd.concat([duration, n_players, event_date], axis=1).reset_index()
    raid_base = raid_base.rename(columns={"raidid": "raid_id"})

    # 3.- Daño y Curación total
    damage = (
        df[df["eventtype"] == "combatdamage"]
        .groupby("raidid")["damageamount"]
        .sum()
        .rename("total_damage")
    )

    healing = (
        df[df["eventtype"] == "heal"]
        .groupby("raidid")["healingamount"]
        .sum()
        .rename("total_healing")
    )

    deaths = (
        df[df["eventtype"] == "playerdeath"]
        .groupby("raidid")["eventtype"]
        .count()
        .rename("total_player_deaths")
    )

    # Juntar con raid_base
    raid_base = (
        raid_base
        .merge(damage, how="left", left_on="raid_id", right_index=True)
        .merge(healing, how="left", left_on="raid_id", right_index=True)
        .merge(deaths, how="left", left_on="raid_id", right_index=True)
    )

    # Añadir counts por rol
    raid_base = raid_base.merge(
        role_counts,
        how="left",
        left_on="raid_id",
        right_on="raidid",
    ).drop(columns=["raidid"])

    # Asegurar tipo entero
    raid_base["n_tanks"] = raid_base["n_tanks"].astype("int64")
    raid_base["n_healers"] = raid_base["n_healers"].astype("int64")
    raid_base["n_dps"] = raid_base["n_dps"].astype("int64")


    # Rellenar NaN por 0 donde toque
    raid_base["total_damage"] = raid_base["total_damage"].fillna(0.0)
    raid_base["total_healing"] = raid_base["total_healing"].fillna(0.0)
    raid_base["total_player_deaths"] = raid_base["total_player_deaths"].fillna(0).astype("int64")

    # 4.- boss_min_hp_pct
    boss_events = df[df["targetentitytype"] == "boss"]

    boss_min = (
        boss_events
        .groupby("raidid")["targetentityhealthpctafter"]
        .min()
        .rename("boss_min_hp_pct")
    )

    raid_base = raid_base.merge(
        boss_min, how="left", left_on="raid_id", right_index=True
    )

    # Si alguna raid no tiene eventos de boss, ponemos 100 como default
    raid_base["boss_min_hp_pct"] = raid_base["boss_min_hp_pct"].fillna(100.0)

    # 5.- DPS y HPS
    # Evitar división por cero
    duration_seconds = raid_base["duration_ms"] / 1000.0
    duration_seconds = duration_seconds.replace(0, np.nan)

    raid_base["raid_dps"] = raid_base["total_damage"] / duration_seconds
    raid_base["raid_hps"] = raid_base["total_healing"] / duration_seconds

    raid_base["raid_dps"] = raid_base["raid_dps"].fillna(0.0)
    raid_base["raid_hps"] = raid_base["raid_hps"].fillna(0.0)

    # 6.- raid_outcome
    T_MAX = 360000  # 6 minutos

    cond_success_kill = (raid_base["boss_min_hp_pct"] == 0.0) & (raid_base["duration_ms"] <= T_MAX)
    cond_success_almost = (
        (raid_base["boss_min_hp_pct"] < 10.0)
        & (raid_base["total_player_deaths"] <= raid_base["n_players"])
    )

    raid_base["raid_outcome"] = np.where(
        cond_success_kill | cond_success_almost,
        "success",
        "wipe",
    )

    # 7.- Selección de columnas
    cols = [
        "raid_id",
        "event_date",
        "duration_ms",
        "total_damage",
        "total_healing",
        "total_player_deaths",
        "n_players",
        "n_tanks",
        "n_healers",
        "n_dps",
        "raid_dps",
        "raid_hps",
        "boss_min_hp_pct",
        "raid_outcome",
    ]

    raid_summary = raid_base[cols].copy()
    return raid_summary

def build_player_raid_stats(
    df_silver: pd.DataFrame,
    raid_summary: pd.DataFrame
) -> pd.DataFrame:
    """
    Construye la tabla gold.player_raid_stats a partir de eventos Silver
    y del resumen de raid (raid_summary).

    Parámetros
    ----------
    df_silver : pd.DataFrame
        Eventos Silver, igual que en build_raid_summary.
    raid_summary : pd.DataFrame
        Resultado de build_raid_summary, usado para obtener duration_ms
        y totales de daño/curación por raid.

    Devuelve
    --------
    pd.DataFrame
        DataFrame con una fila por (raid_id, player_id) y columnas:
        raid_id, event_date,
        player_id, player_name, player_class, player_role,
        damage_total, healing_total,
        damage_events, healing_events,
        player_deaths, crit_events, crit_rate,
        total_damage_received,
        dps, hps,
        damage_share, healing_share,
        deaths_per_10min (opcional),
        role_activity_flag (opcional).
    """
    # 1. Copia defensiva
    df = df_silver.copy()
    
    # 2. Agrupar por raid + jugador para agregados básicos
    group = df.groupby(["raidid", "sourceplayerid"])
    
    # Daño total por jugador
    damage_per_player = (
        df[df["eventtype"] == "combatdamage"]
        .groupby(["raidid", "sourceplayerid"])["damageamount"]
        .sum()
        .rename("damage_total")
    )

    # Contar eventos de daño y curación
    damage_events = (
        df[df["eventtype"] == "combatdamage"]
        .groupby(["raidid", "sourceplayerid"])["eventtype"]
        .count()
        .rename("damage_events")
    )
    
    # Curación total por jugador
    healing_per_player = (
        df[df["eventtype"] == "heal"]
        .groupby(["raidid", "sourceplayerid"])["healingamount"]
        .sum()
        .rename("healing_total")
    )

    healing_events = (
        df[df["eventtype"] == "heal"]
        .groupby(["raidid", "sourceplayerid"])["eventtype"]
        .count()
        .rename("healing_events")
    )
    
    # Muertes del jugador (cuando el jugador es TARGET de playerdeath)
    # Cuidado: en eventos de muerte, el target es el muerto, no el source
    # Ajusta según tu schema: si usas targetentityid para el muerto,
    # agrupa por (raidid, targetentityid). Si tu schema usa sourceplayerid
    # como el que muere, usa sourceplayerid.
    
    # Supongo que en tu schema el muerto es sourceplayerid en eventtype playerdeath
    player_deaths = (
        df[df["eventtype"] == "playerdeath"]
        .groupby(["raidid", "sourceplayerid"])["eventtype"]
        .count()
        .rename("player_deaths")
    )
    
    # 3. Metadatos del jugador (name, class, role) - tomar primer valor
    player_meta = (
        df.groupby(["raidid", "sourceplayerid"])
        .agg({
            "sourceplayername": "first",
            "sourceplayerclass": "first",
            "sourceplayerrole": "first",
        })
        .rename(columns={
            "sourceplayername": "player_name",
            "sourceplayerclass": "player_class",
            "sourceplayerrole": "player_role",
        })
    )
    
    # 4. Construir base
    player_base = player_meta.copy()
    player_base = player_base.reset_index().rename(columns={
        "raidid": "raid_id",
        "sourceplayerid": "player_id",
    })
    
    # 5. Añadir damage, healing, deaths
    player_base = (
        player_base
        .merge(damage_per_player, how="left", left_on=["raid_id", "player_id"], right_index=True)
        .merge(healing_per_player, how="left", left_on=["raid_id", "player_id"], right_index=True)
        .merge(player_deaths, how="left", left_on=["raid_id", "player_id"], right_index=True)
        .merge(damage_events, how="left", left_on=["raid_id", "player_id"], right_index=True)
        .merge(healing_events, how="left", left_on=["raid_id", "player_id"], right_index=True)
    )
    
    # Rellenar NaN
    player_base["damage_total"] = player_base["damage_total"].fillna(0.0)
    player_base["healing_total"] = player_base["healing_total"].fillna(0.0)
    player_base["player_deaths"] = player_base["player_deaths"].fillna(0).astype("int64")
    player_base["damage_events"] = player_base["damage_events"].fillna(0).astype("int64")
    player_base["healing_events"] = player_base["healing_events"].fillna(0).astype("int64")
    
    # 6. Añadir event_date y duration_ms desde raid_summary
    player_base = player_base.merge(
        raid_summary[["raid_id", "event_date", "duration_ms"]],
        how="left",
        on="raid_id",
    )
    
    # 7. Calcular DPS y HPS
    duration_seconds = player_base["duration_ms"] / 1000.0
    duration_seconds = duration_seconds.replace(0, np.nan)
    
    player_base["dps"] = player_base["damage_total"] / duration_seconds
    player_base["hps"] = player_base["healing_total"] / duration_seconds
    
    player_base["dps"] = player_base["dps"].fillna(0.0)
    player_base["hps"] = player_base["hps"].fillna(0.0)

    # 8. Contar eventos críticos (iscriticalhit == True)
    crit_events = (
        df[(df["eventtype"].isin(["combatdamage", "heal"])) & (df["iscriticalhit"] == True)]
        .groupby(["raidid", "sourceplayerid"])["eventtype"]
        .count()
        .rename("crit_events")
    )

    # Merge
    player_base = player_base.merge(
        crit_events, how="left", left_on=["raid_id", "player_id"], right_index=True
    )
    player_base["crit_events"] = player_base["crit_events"].fillna(0).astype("int64")

    # Calcular crit_rate
    total_events = player_base["damage_events"] + player_base["healing_events"]
    player_base["crit_rate"] = np.where(
        total_events > 0,
        player_base["crit_events"] / total_events,
        0.0
    )

    damage_received = (
        df[df["eventtype"] == "combatdamage"]
        .groupby(["raidid", "targetentityid"])["damageamount"]
        .sum()
        .rename("total_damage_received")
    )

    # Merge usando targetentityid = player_id
    player_base = player_base.merge(
        damage_received,
        how="left",
        left_on=["raid_id", "player_id"],
        right_index=True,
    )
    player_base["total_damage_received"] = player_base["total_damage_received"].fillna(0.0)

    # Merge con raid_summary para obtener totales de raid
    player_base = player_base.merge(
        raid_summary[["raid_id", "total_damage", "total_healing"]],
        how="left",
        on="raid_id",
    )

    # Calcular shares (las columnas de raid se llaman total_damage y total_healing)
    player_base["damage_share"] = np.where(
        player_base["total_damage"] > 0,
        player_base["damage_total"] / player_base["total_damage"],
        0.0
    )

    player_base["healing_share"] = np.where(
        player_base["total_healing"] > 0,
        player_base["healing_total"] / player_base["total_healing"],
        0.0
    )

    # Limpiar columnas auxiliares (ahora se llaman total_damage y total_healing)
    player_base = player_base.drop(columns=["total_damage", "total_healing"])



    # . Seleccionar columnas
    cols = [
        "raid_id",
        "event_date",
        "player_id",
        "player_name",
        "player_class",
        "player_role",
        "damage_total",
        "healing_total",
        "damage_events",
        "healing_events",
        "player_deaths",
        "crit_events",
        "crit_rate",
        "total_damage_received",
        "dps",
        "hps",
        "damage_share",
        "healing_share",
    ]

    
    player_stats = player_base[cols].copy()
    
    return player_stats

def apply_raid_outcome_rule(raid_summary: pd.DataFrame) -> pd.DataFrame:
    """
    Aplica la regla combinada de raid_outcome sobre raid_summary.

    Regla:
        - success si:
            boss_min_hp_pct == 0 y duration_ms <= 360000
          o bien:
            boss_min_hp_pct < 10 y total_player_deaths <= n_players
        - wipe en caso contrario.
    """
    ...
