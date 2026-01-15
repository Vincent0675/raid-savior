# src/generators/raid_event_generator.py

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Optional
import random
import uuid

import numpy as np

from src.schemas.eventos_schema import WoWRaidEvent, EventType


@dataclass(frozen=True)
class Player:
    player_id: str
    name: str
    role: str          # "tank" | "healer" | "dps" (Pydantic puede coercionar a Enum)
    player_class: str  # "mage", "priest", etc.
    level: int = 90


@dataclass(frozen=True)
class RaidSession:
    raid_id: str
    boss_id: str
    boss_name: str
    start_time: datetime
    end_time: datetime
    players: list[Player]


class WoWEventGenerator:
    def __init__(self, seed: int = 42) -> None:
        self.seed = seed
        self._rng = np.random.default_rng(seed)
        self._py_rng = random.Random(seed)

        # “catálogo” mínimo de abilities (strings para que Pydantic las convierta si usa Enums)
        self._damage_abilities = [
            {"ability_id": "spell001", "ability_name": "Fireball", "ability_school": "fire"},
            {"ability_id": "spell002", "ability_name": "Pyroblast", "ability_school": "fire"},
            {"ability_id": "spell003", "ability_name": "Frostbolt", "ability_school": "frost"},
        ]
        self._heal_abilities = [
            {"ability_id": "spell101", "ability_name": "Healing Touch", "ability_school": "holy"},
            {"ability_id": "spell102", "ability_name": "Flash Heal", "ability_school": "holy"},
            {"ability_id": "spell103", "ability_name": "Renew", "ability_school": "holy"},
        ]

        # clases de ejemplo (suficiente para tests; luego podrás meter Faker)
        self._classes = [
            "warrior", "paladin", "hunter", "rogue", "priest",
            "shaman", "mage", "warlock", "druid"
        ]

    def _resolve_event_type(self, *candidates: str) -> EventType:
        """
        Evita acoplarte a cómo nombraste el Enum.
        Ej: EventType.DAMAGE vs EventType.COMBAT_DAMAGE.
        """
        for name in candidates:
            if hasattr(EventType, name):
                return getattr(EventType, name)
        raise ValueError(f"No EventType candidate found in Enum: {candidates}")

    def generate_raid_session(
        self,
        raid_id: str = "raid001",
        num_players: int = 20,
        duration_s: int = 300,
        start_time: Optional[datetime] = None,
    ) -> RaidSession:
        # Importante: start_time en el pasado para no violar "timestamp no futuro"
        if start_time is None:
            start_time = datetime.now(timezone.utc) - timedelta(minutes=10)

        end_time = start_time + timedelta(seconds=duration_s)

        players: list[Player] = []
        # distribución típica: 8% tank, 20% healer, 72% dps
        role_probs = [0.08, 0.20, 0.72]
        role_names = ["tank", "healer", "dps"]

        for i in range(num_players):
            role = self._rng.choice(role_names, p=role_probs)
            pclass = self._py_rng.choice(self._classes)
            name = f"Player_{i:02d}"
            player_id = f"player_{uuid.uuid4().hex[:8]}"
            players.append(Player(player_id=player_id, name=name, role=role, player_class=pclass, level=90))

        return RaidSession(
            raid_id=raid_id,
            boss_id="boss001",
            boss_name="Ragnaros",
            start_time=start_time,
            end_time=end_time,
            players=players,
        )

    def generate_events(self, session: RaidSession, num_events: int = 1000) -> list[WoWRaidEvent]:
        start_ts = session.start_time.timestamp()
        end_ts = session.end_time.timestamp()

        # timestamps uniformes y ordenados
        timestamps = np.linspace(start_ts, end_ts, num_events)

        events: list[WoWRaidEvent] = []
        for ts in timestamps:
            timestamp = datetime.fromtimestamp(float(ts), tz=timezone.utc)

            # ratio simple para v0: 70% daño, 30% heal
            if self._rng.random() < 0.70:
                ev = self._create_damage_event(session, timestamp)
            else:
                ev = self._create_heal_event(session, timestamp)

            events.append(ev)

        return events

    def _pick_player(self, session: RaidSession, role: str) -> Player:
        candidates = [p for p in session.players if p.role == role]
        if candidates:
            return self._py_rng.choice(candidates)
        # fallback (no debería pasar con probs normales)
        return self._py_rng.choice(session.players)

    def _latency_ms(self, mean: float, std: float) -> int:
        v = float(self._rng.normal(loc=mean, scale=std))
        return int(max(0.0, v))

    def _create_damage_event(self, session: RaidSession, timestamp: datetime) -> WoWRaidEvent:
        event_type_damage = self._resolve_event_type("COMBAT_DAMAGE", "DAMAGE")
        source = self._pick_player(session, role="dps")
        ability = self._py_rng.choice(self._damage_abilities)

        base_damage = float(self._rng.normal(loc=15000, scale=3000))
        damage = float(np.clip(base_damage, 5000, 50000))

        # críticos: 15% para DPS
        is_crit = bool(self._rng.binomial(n=1, p=0.15))
        crit_mult = float(self._rng.uniform(1.3, 2.0)) if is_crit else 1.0

        encounter_duration_ms = int((timestamp - session.start_time).total_seconds() * 1000)

        return WoWRaidEvent(
            event_id=uuid.uuid4(),
            event_type=event_type_damage,
            timestamp=timestamp,
            raid_id=session.raid_id,

            encounter_id=f"{session.boss_id}_encounter",
            encounter_duration_ms=encounter_duration_ms,

            source_player_id=source.player_id,
            source_player_name=source.name,
            source_player_role=source.role,
            source_player_class=source.player_class,
            source_player_level=source.level,

            target_entity_id=session.boss_id,
            target_entity_name=session.boss_name,
            target_entity_type="boss",

            target_entity_health_pct_before=float(self._rng.uniform(1, 100)),
            target_entity_health_pct_after=float(self._rng.uniform(1, 100)),

            ability_id=ability["ability_id"],
            ability_name=ability["ability_name"],
            ability_school=ability["ability_school"],

            damage_amount=damage,
            healing_amount=None,

            is_critical_hit=is_crit,
            critical_multiplier=crit_mult,

            is_resisted=False,
            is_blocked=False,
            is_absorbed=False,

            server_latency_ms=self._latency_ms(45, 10),
            client_latency_ms=self._latency_ms(50, 15),

            ingestion_timestamp=datetime.now(timezone.utc),
            source_system="wow-raid-addon-v1.2",
            data_quality_flags=[],
        )

    def _create_heal_event(self, session: RaidSession, timestamp: datetime) -> WoWRaidEvent:
        event_type_heal = self._resolve_event_type("HEAL", "HEALING")
        healer = self._pick_player(session, role="healer")
        target = self._py_rng.choice(session.players)
        ability = self._py_rng.choice(self._heal_abilities)

        base_heal = float(self._rng.normal(loc=8500, scale=2000))
        healing = float(np.clip(base_heal, 2000, 25000))

        # críticos healer (bajo): 5%
        is_crit = bool(self._rng.binomial(n=1, p=0.05))
        crit_mult = float(self._rng.uniform(1.2, 1.8)) if is_crit else 1.0

        encounter_duration_ms = int((timestamp - session.start_time).total_seconds() * 1000)

        return WoWRaidEvent(
            event_id=uuid.uuid4(),
            event_type=event_type_heal,
            timestamp=timestamp,
            raid_id=session.raid_id,

            encounter_id=f"{session.boss_id}_encounter",
            encounter_duration_ms=encounter_duration_ms,

            source_player_id=healer.player_id,
            source_player_name=healer.name,
            source_player_role=healer.role,
            source_player_class=healer.player_class,
            source_player_level=healer.level,

            target_entity_id=target.player_id,
            target_entity_name=target.name,
            target_entity_type="player",

            target_entity_health_pct_before=float(self._rng.uniform(30, 100)),
            target_entity_health_pct_after=float(self._rng.uniform(50, 100)),

            ability_id=ability["ability_id"],
            ability_name=ability["ability_name"],
            ability_school=ability["ability_school"],

            damage_amount=None,
            healing_amount=healing,

            is_critical_hit=is_crit,
            critical_multiplier=crit_mult,

            is_resisted=False,
            is_blocked=False,
            is_absorbed=False,

            server_latency_ms=self._latency_ms(45, 10),
            client_latency_ms=self._latency_ms(50, 15),

            ingestion_timestamp=datetime.now(timezone.utc),
            source_system="wow-raid-addon-v1.2",
            data_quality_flags=[],
        )
