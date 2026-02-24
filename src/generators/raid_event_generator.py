# src/generators/raid_event_generator.py
"""
WoW Raid Event Generator - Enhanced Version
Genera eventos sintéticos con patrones realistas para Big Data testing.

Versión: 2.0 (Mejorado para Fase 4)
Cambios:
- 6 tipos de eventos (combat_damage, heal, player_death, spell_cast, boss_phase, mana_regeneration)
- Fases de boss con mecánicas dinámicas
- Patrones temporales no uniformes (burst windows)
- Volumen escalable (100k-1M eventos)
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Optional, List
import random
import uuid

import numpy as np

from src.schemas.eventos_schema import (
    WoWRaidEvent, EventType, PlayerRole, PlayerClass, 
    DamageSchool, EntityType, ResourceType
)

from src.generators.class_profiles import SPEC_PROFILES
from src.generators.class_profiles import SPECS_BY_ROLE, RAID_ROLE_WEIGHTS
from src.schemas.eventos_schema import ResourceType


@dataclass(frozen=True)
class Player:
    # Campos que SÍ se pasan al constructor
    player_id: str
    name: str
    player_class: str   # e.g. "mage"
    spec: str           # e.g. "fire"  ← NUEVO campo principal
    level: int = 90
    # Campos derivados — init=False significa que NO se pasan al constructor
    # Los calcula __post_init__ consultando SPEC_PROFILES
    role: str          = field(default="",    init=False)
    dps_type: str|None = field(default=None,  init=False)
    resource_type: str = field(default="",    init=False)

    def __post_init__(self) -> None:
        """Deriva role, dps_type y resource_type de SPEC_PROFILES."""
        spec_key = (self.player_class, self.spec)

        # Validación defensiva: si la spec no existe, falla rápido (fail-fast)
        if spec_key not in SPEC_PROFILES:
            raise ValueError(
                f"Spec desconocida: {spec_key}. "
                f"Claves válidas: {list(SPEC_PROFILES.keys())[:5]}..."
            )

        profile = SPEC_PROFILES[spec_key]

        # object.__setattr__ porque frozen=True no permite self.campo = valor
        object.__setattr__(self, "role",          profile["role"])
        object.__setattr__(self, "dps_type",      profile["dps_type"])
        object.__setattr__(self, "resource_type", profile["resource_type"])

@dataclass
class BossPhase:
    """Representa una fase del boss encounter."""
    phase_number: int
    phase_name: str
    duration_s: int
    damage_multiplier: float = 1.0
    healing_multiplier: float = 1.0
    death_probability: float = 0.0

@dataclass
class BossHPTracker:
    """Representa el seguimiento de la salud del boss."""
    max_hp: float
    phase_thresholds: list[float] = field(
        default_factory=lambda: [100.0, 70.0, 40.0, 0.0]
    )
    # Campos de estado - init=False: no se pasan al constructor
    current_hp:          float = field(default=0.0, init=False)
    current_phase_index: int   = field(default=0,   init=False)
    
    def __post_init__(self) -> None:
        self.current_hp          = self.max_hp # empieza con HP completo
        self.current_phase_index = 0           # empieza en fase 1 (es decir, índice 0)

    @property
    def hp_pct(self) -> float:
        return (self.current_hp / self.max_hp) * 100.0

    @property
    def current_phase_number(self) -> int:
        return self.current_phase_index + 1 # índice 0 -> fase 1

    def register_damage(self, amount: float) -> bool:
        self.current_hp = max(0.0, self.current_hp - amount)
        
        max_phase = len(self.phase_thresholds) - 2
        if self.current_phase_index < max_phase:
            next_threshold = self.phase_thresholds[self.current_phase_index + 1]
            if self.hp_pct <= next_threshold:
                self.current_phase_index += 1
                return True
        
        return False


@dataclass(frozen=True)
class RaidSession:
    raid_id: str
    boss_id: str
    boss_name: str
    start_time: datetime
    end_time: datetime
    players: List[Player]
    phases: List[BossPhase] = field(default_factory=list)


class WoWEventGenerator:
    """
    Generador de eventos sintéticos con distribuciones realistas.
    Compatible con Pydantic v2 + schema validation.
    """
    
    def __init__(self, seed: int = 42) -> None:
        self.seed = seed
        self._rng = np.random.default_rng(seed)
        self._py_rng = random.Random(seed)

        # Catálogo de habilidades

        self._boss_abilities = [
            {"ability_id": "boss001", "ability_name": "Lava Burst", "ability_school": "fire"},
            {"ability_id": "boss002", "ability_name": "Shadow Strike", "ability_school": "shadow"},
            {"ability_id": "boss003", "ability_name": "AOE Flame Wave", "ability_school": "fire"},
        ]

    def generate_raid_session(
        self,
        raid_id: str = "raid001",
        num_players: int = 20,
        duration_s: int = 300,
        start_time: Optional[datetime] = None,
        boss_name: str = "Ragnaros",
    ) -> RaidSession:
        """Genera una sesión de raid con fases de boss."""
        
        if start_time is None:
            # Importante: En el pasado para validación de timestamp
            start_time = datetime.now(timezone.utc) - timedelta(hours=1)
        
        end_time = start_time + timedelta(seconds=duration_s)
        
        players: List[Player] = [] 

        role_names   = list(RAID_ROLE_WEIGHTS.keys())    # ["tank", "healer", "dps"]
        role_weights = list(RAID_ROLE_WEIGHTS.values())  # [0.10,   0.20,     0.70]

        for i in range(num_players):
            # Paso 1: samplear rol
            role = str(self._rng.choice(role_names, p=role_weights))

            # Paso 2: samplear spec dentro de ese rol
            # SPECS_BY_ROLE["dps"] es una lista de tuplas: [("mage","fire"), ("rogue","combat"), ...]
            available_specs = SPECS_BY_ROLE[role]
            spec_index = self._py_rng.randrange(len(available_specs))
            player_class, spec_name = available_specs[spec_index]

            # Paso 3: construir Player — role/dps_type/resource_type se derivan en __post_init__
            players.append(Player(
                player_id=f"player_{uuid.uuid4().hex[:8]}",
                name=f"Player_{i:02d}",
                player_class=player_class,
                spec=spec_name,
                level=90,
            ))
            
        # Generar fases del boss (3 fases típicas)
        phases = [
            BossPhase(
                phase_number=1,
                phase_name="Phase 1: Opening",
                duration_s=int(duration_s * 0.35),
                damage_multiplier=1.0,
                healing_multiplier=1.0,
                death_probability=0.02,
            ),
            BossPhase(
                phase_number=2,
                phase_name="Phase 2: Enrage",
                duration_s=int(duration_s * 0.40),
                damage_multiplier=1.5,
                healing_multiplier=1.8,
                death_probability=0.08,
            ),
            BossPhase(
                phase_number=3,
                phase_name="Phase 3: Burn",
                duration_s=int(duration_s * 0.25),
                damage_multiplier=2.0,
                healing_multiplier=2.5,
                death_probability=0.15,
            ),
        ]
        
        return RaidSession(
            raid_id=raid_id,
            boss_id=f"boss_{boss_name.lower().replace(' ', '_')}",
            boss_name=boss_name,
            start_time=start_time,
            end_time=end_time,
            players=players,
            phases=phases,
        )

    def generate_events(
        self, 
        session: RaidSession, 
        num_events: int = 1000,
        event_distribution: Optional[dict] = None
    ) -> List[WoWRaidEvent]:
        """
        Genera eventos distribuidos por fases del boss.
        
        Args:
            session: Sesión de raid
            num_events: Número total de eventos
            event_distribution: Distribución de tipos de evento
        """
        
        # Distribución default
        if event_distribution is None:
            event_distribution = {
                "damage": 0.45,
                "heal": 0.30,
                "spell_cast": 0.15,
                "mana_regen": 0.05,
                "boss_phase": 0.03,
                "player_death": 0.02
            }
        
        events: List[WoWRaidEvent] = []
        
        # Si no hay fases, generar con patrón uniforme (backward compatibility)
        if not session.phases:
            return self._generate_simple_events(session, num_events)
        
        # Calcular max_hp proporcional al daño esperado de la raid
        avg_damage_per_event    = 15_000
        estimated_damage_events = int(num_events * 0.50)
        max_hp = avg_damage_per_event * estimated_damage_events * 1.3

        # Instanciar tracker y generar todos los timestamps de golpe
        boss_tracker = BossHPTracker(max_hp=max_hp)

        start_ts = session.start_time.timestamp()
        end_ts   = session.end_time.timestamp()

        all_timestamps = self._generate_realistic_timestamps(
            start_ts=start_ts,
            end_ts=end_ts,
            count=num_events,
            burst_intensity=0.5
        )

        for ts in all_timestamps:
            timestamp = datetime.fromtimestamp(float(ts), tz=timezone.utc)

            # Fase actual según HP del boss — no según tiempo
            phase_index = min(boss_tracker.current_phase_index, len(session.phases) - 1)
            phase       = session.phases[phase_index]

            # 1. Elegir jugador
            player = self._pick_player(session)

            # 2. Obtener pesos de su spec y samplear event_type
            weights     = SPEC_PROFILES[(player.player_class, player.spec)]["event_weights"]
            event_types = list(weights.keys())
            event_probs = list(weights.values())
            etype       = str(self._rng.choice(event_types, p=event_probs))

            # 3. Dispatch
            if etype == "combat_damage":
                hp_before_hit = boss_tracker.hp_pct
                ev = self._create_damage_event(player, session, timestamp, phase)
                if ev.damage_amount:
                    phase_changed = boss_tracker.register_damage(ev.damage_amount)
                    if phase_changed:
                        new_phase_index = min(boss_tracker.current_phase_index, len(session.phases) - 1)
                        new_phase       = session.phases[new_phase_index]
                        events.append(self._create_boss_phase_event(
                            session,
                            timestamp,
                            new_phase,
                            hp_pct_before=hp_before_hit,
                            hp_pct_after=boss_tracker.hp_pct
                            ))
            elif etype == "heal":
                ev = self._create_heal_event(player, session, timestamp, phase)
            elif etype == "spell_cast":
                ev = self._create_spell_cast_event(player, session, timestamp, phase)
            elif etype == "mana_regen":
                ev = self._create_mana_regen_event(player, session, timestamp)
            elif etype == "player_death":
                ev = self._create_player_death_event(player, session, timestamp, phase)
            else:
                continue

            events.append(ev)
        return events

    def _generate_simple_events(self, session: RaidSession, num_events: int) -> List[WoWRaidEvent]:
        """Generación simple (backward compatibility con v1)."""
        start_ts = session.start_time.timestamp()
        end_ts = session.end_time.timestamp()
        timestamps = np.linspace(start_ts, end_ts, num_events)
        
        # Crear fase dummy
        dummy_phase = BossPhase(
            phase_number=1,
            phase_name="Single Phase",
            duration_s=int((session.end_time - session.start_time).total_seconds()),
            damage_multiplier=1.0,
            healing_multiplier=1.0,
            death_probability=0.0
        )
        
        events: List[WoWRaidEvent] = []
        for ts in timestamps:
            timestamp = datetime.fromtimestamp(float(ts), tz=timezone.utc)
            
            if self._rng.random() < 0.70:
                ev = self._create_damage_event(session, timestamp, dummy_phase)
            else:
                ev = self._create_heal_event(session, timestamp, dummy_phase)
            
            events.append(ev)
        
        return events

    def _generate_realistic_timestamps(
        self, 
        start_ts: float, 
        end_ts: float, 
        count: int,
        burst_intensity: float = 0.5
    ) -> np.ndarray:
        """Genera timestamps con distribución no uniforme."""
        beta_samples = self._rng.beta(a=2, b=2 - burst_intensity, size=count)
        timestamps = start_ts + beta_samples * (end_ts - start_ts)
        return np.sort(timestamps)

    def _pick_player(self, session: RaidSession, role: Optional[str] = None) -> Player:
        """Selecciona un jugador (opcionalmente por rol)."""
        if role:
            candidates = [p for p in session.players if p.role == role]
            if candidates:
                return self._py_rng.choice(candidates)
        return self._py_rng.choice(session.players)

    def _latency_ms(self, mean: float, std: float) -> int:
        """Genera latencia con distribución normal."""
        v = float(self._rng.normal(loc=mean, scale=std))
        return int(max(0.0, v))
    
    def _get_ability(self, player: Player, ability_type: str) -> dict:
        """
        Devuelve una ability aleatoria del catálogo de la spec del jugador.
        
        Args:
            player: El jugador que ejecuta la ability
            ability_type: "damage" o "heal"
        
        Returns:
            dict con ability_id, ability_name, ability_school
        """
        spec_key = (player.player_class, player.spec)
        abilities = SPEC_PROFILES[spec_key]["abilities"][ability_type]
        
        # Fallback: si la lista está vacía (ej. un healer pidiendo damage abilities
        # y no tiene ninguna), devolvemos una ability genérica
        if not abilities:
            return {"ability_id": "generic_001", "ability_name": "Auto Attack", "ability_school": "physical"}
        
        return self._py_rng.choice(abilities)


    # ===== CREADORES DE EVENTOS =====

    def _create_damage_event(
        self,
        player: Player,
        session: RaidSession, 
        timestamp: datetime, 
        phase: BossPhase
    ) -> WoWRaidEvent:
        """Genera evento de daño."""
        source = player
        ability = self._get_ability(player, "damage")
        
        base_damage = float(self._rng.normal(loc=15000, scale=3000))
        damage = float(np.clip(base_damage * phase.damage_multiplier, 5000, 100000))
        
        is_crit = bool(self._rng.binomial(n=1, p=0.18))
        crit_mult = float(self._rng.uniform(1.5, 2.2)) if is_crit else 1.0
        
        encounter_duration_ms = int((timestamp - session.start_time).total_seconds() * 1000)
        
        return WoWRaidEvent(
            event_id=uuid.uuid4(),
            event_type=EventType.COMBAT_DAMAGE,
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
            target_entity_type=EntityType.BOSS,
            target_entity_health_pct_before=float(self._rng.uniform(10, 100)),
            target_entity_health_pct_after=float(self._rng.uniform(5, 99)),
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
            source_system="wow-raid-addon-v2.0",
            data_quality_flags=[],
        )

    def _create_heal_event(
        self, 
        player: Player,
        session: RaidSession, 
        timestamp: datetime, 
        phase: BossPhase
    ) -> WoWRaidEvent:
        """Genera evento de curación."""
        healer = player
        target = self._py_rng.choice(session.players)
        ability = ability = self._get_ability(player, "heal")
        
        base_heal = float(self._rng.normal(loc=9500, scale=2500))
        healing = float(np.clip(base_heal * phase.healing_multiplier, 3000, 40000))
        
        is_crit = bool(self._rng.binomial(n=1, p=0.12))
        crit_mult = float(self._rng.uniform(1.3, 1.9)) if is_crit else 1.0
        
        encounter_duration_ms = int((timestamp - session.start_time).total_seconds() * 1000)
        
        return WoWRaidEvent(
            event_id=uuid.uuid4(),
            event_type=EventType.HEAL,
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
            target_entity_type=EntityType.PLAYER,
            target_entity_health_pct_before=float(self._rng.uniform(20, 85)),
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
            source_system="wow-raid-addon-v2.0",
            data_quality_flags=[],
        )

    def _create_spell_cast_event(
        self, 
        player: Player,
        session: RaidSession, 
        timestamp: datetime, 
        phase: BossPhase
    ) -> WoWRaidEvent:
        """Genera evento de spell_cast."""
        caster = player
        ability_type = "heal" if player.role == "healer" else "damage"
        ability = self._get_ability(player, ability_type)

        encounter_duration_ms = int((timestamp - session.start_time).total_seconds() * 1000)
        
        return WoWRaidEvent(
            event_id=uuid.uuid4(),
            event_type=EventType.SPELL_CAST,
            timestamp=timestamp,
            raid_id=session.raid_id,
            encounter_id=f"{session.boss_id}_encounter",
            encounter_duration_ms=encounter_duration_ms,
            source_player_id=caster.player_id,
            source_player_name=caster.name,
            source_player_role=caster.role,
            source_player_class=caster.player_class,
            source_player_level=caster.level,
            target_entity_id=session.boss_id,
            target_entity_name=session.boss_name,
            target_entity_type=EntityType.BOSS,
            ability_id=ability["ability_id"],
            ability_name=ability["ability_name"],
            ability_school=ability["ability_school"],
            damage_amount=None,
            healing_amount=None,
            is_critical_hit=False,
            critical_multiplier=1.0,
            is_resisted=False,
            is_blocked=False,
            is_absorbed=False,
            server_latency_ms=self._latency_ms(45, 10),
            client_latency_ms=self._latency_ms(50, 15),
            ingestion_timestamp=datetime.now(timezone.utc),
            source_system="wow-raid-addon-v2.0",
            data_quality_flags=[],
        )

    def _create_mana_regen_event(
        self,
        player: Player, 
        session: RaidSession, 
        timestamp: datetime
    ) -> WoWRaidEvent:
        """Genera evento de regeneración de maná."""
        caster = player
        
        mana_before = float(self._rng.uniform(20, 80))
        mana_regen_rate = float(self._rng.uniform(50, 150))
        mana_after = min(100.0, mana_before + (mana_regen_rate / 10))
        
        encounter_duration_ms = int((timestamp - session.start_time).total_seconds() * 1000)
        
        return WoWRaidEvent(
            event_id=uuid.uuid4(),
            event_type=EventType.MANA_REGENERATION,
            timestamp=timestamp,
            raid_id=session.raid_id,
            encounter_id=f"{session.boss_id}_encounter",
            encounter_duration_ms=encounter_duration_ms,
            source_player_id=caster.player_id,
            source_player_name=caster.name,
            source_player_role=caster.role,
            source_player_class=caster.player_class,
            source_player_level=caster.level,
            target_entity_id=caster.player_id,
            target_entity_name=caster.name,
            target_entity_type=EntityType.PLAYER,
            resource_type=ResourceType(player.resource_type),
            resource_amount_before=mana_before,
            resource_amount_after=mana_after,
            resource_regeneration_rate=mana_regen_rate,
            damage_amount=None,
            healing_amount=None,
            is_critical_hit=False,
            critical_multiplier=1.0,
            is_resisted=False,
            is_blocked=False,
            is_absorbed=False,
            server_latency_ms=self._latency_ms(45, 10),
            client_latency_ms=self._latency_ms(50, 15),
            ingestion_timestamp=datetime.now(timezone.utc),
            source_system="wow-raid-addon-v2.0",
            data_quality_flags=[],
        )

    def _create_player_death_event(
        self, 
        victim: Player, 
        session: RaidSession, 
        timestamp: datetime, 
        phase: BossPhase
    ) -> WoWRaidEvent:
        """Genera evento de muerte de jugador."""
        if self._rng.random() > phase.death_probability:
            healer = self._pick_player(session, role="healer")
            return self._create_heal_event(healer, session, timestamp, phase)
        
        victim = self._py_rng.choice(session.players)
        boss_ability = self._py_rng.choice(self._boss_abilities)
        
        encounter_duration_ms = int((timestamp - session.start_time).total_seconds() * 1000)
        
        return WoWRaidEvent(
            event_id=uuid.uuid4(),
            event_type=EventType.PLAYER_DEATH,
            timestamp=timestamp,
            raid_id=session.raid_id,
            encounter_id=f"{session.boss_id}_encounter",
            encounter_duration_ms=encounter_duration_ms,
            source_player_id=session.boss_id,
            source_player_name=session.boss_name,
            source_player_role=None,
            source_player_class=None,
            source_player_level=None,
            target_entity_id=victim.player_id,
            target_entity_name=victim.name,
            target_entity_type=EntityType.PLAYER,
            target_entity_health_pct_before=float(self._rng.uniform(0, 25)),
            target_entity_health_pct_after=0.0,
            ability_id=boss_ability["ability_id"],
            ability_name=boss_ability["ability_name"],
            ability_school=boss_ability["ability_school"],
            damage_amount=None,
            healing_amount=None,
            is_critical_hit=False,
            critical_multiplier=1.0,
            is_resisted=False,
            is_blocked=False,
            is_absorbed=False,
            server_latency_ms=self._latency_ms(45, 10),
            client_latency_ms=self._latency_ms(50, 15),
            ingestion_timestamp=datetime.now(timezone.utc),
            source_system="wow-raid-addon-v2.0",
            data_quality_flags=["player_death"],
        )

    def _create_boss_phase_event(
        self, 
        session: RaidSession, 
        timestamp: datetime, 
        phase: BossPhase,
        hp_pct_before: float | None = None,
        hp_pct_after: float | None = None,
    ) -> WoWRaidEvent:
        """Genera evento de transición de fase del boss."""

        hp_before = hp_pct_before if hp_pct_before is not None else float(100 - (phase.phase_number - 1) * 33)
        hp_after  = hp_pct_after if hp_pct_after is not None else float(100 - phase.phase_number * 33)

        encounter_duration_ms = int((timestamp - session.start_time).total_seconds() * 1000)
        
        return WoWRaidEvent(
            event_id=uuid.uuid4(),
            event_type=EventType.BOSS_PHASE,
            timestamp=timestamp,
            raid_id=session.raid_id,
            encounter_id=f"{session.boss_id}_encounter",
            encounter_duration_ms=encounter_duration_ms,
            source_player_id=session.boss_id,
            source_player_name=session.boss_name,
            source_player_role=None,
            source_player_class=None,
            source_player_level=None,
            target_entity_id=session.boss_id,
            target_entity_name=session.boss_name,
            target_entity_type=EntityType.BOSS,
            target_entity_health_pct_before=hp_before,
            target_entity_health_pct_after=hp_after,
            damage_amount=None,
            healing_amount=None,
            is_critical_hit=False,
            critical_multiplier=1.0,
            is_resisted=False,
            is_blocked=False,
            is_absorbed=False,
            server_latency_ms=self._latency_ms(45, 10),
            client_latency_ms=self._latency_ms(50, 15),
            ingestion_timestamp=datetime.now(timezone.utc),
            source_system="wow-raid-addon-v2.0",
            data_quality_flags=[f"boss_phase_{phase.phase_number}"],
        )
