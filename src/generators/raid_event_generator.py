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


@dataclass(frozen=True)
class Player:
    player_id: str
    name: str
    role: str  # Mantenemos como string para compatibilidad con NumPy
    player_class: str
    level: int = 90
    current_health_pct: float = field(default=100.0)
    current_mana_pct: float = field(default=100.0)


@dataclass
class BossPhase:
    """Representa una fase del boss encounter."""
    phase_number: int
    phase_name: str
    duration_s: int
    damage_multiplier: float = 1.0
    healing_multiplier: float = 1.0
    death_probability: float = 0.0


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
        self._damage_abilities = [
            {"ability_id": "spell001", "ability_name": "Fireball", "ability_school": "fire"},
            {"ability_id": "spell002", "ability_name": "Pyroblast", "ability_school": "fire"},
            {"ability_id": "spell003", "ability_name": "Frostbolt", "ability_school": "frost"},
            {"ability_id": "spell004", "ability_name": "Shadowbolt", "ability_school": "shadow"},
            {"ability_id": "spell005", "ability_name": "Arcane Missiles", "ability_school": "arcane"},
            {"ability_id": "spell006", "ability_name": "Mortal Strike", "ability_school": "physical"},
        ]
        
        self._heal_abilities = [
            {"ability_id": "spell101", "ability_name": "Healing Touch", "ability_school": "holy"},
            {"ability_id": "spell102", "ability_name": "Flash Heal", "ability_school": "holy"},
            {"ability_id": "spell103", "ability_name": "Renew", "ability_school": "holy"},
            {"ability_id": "spell104", "ability_name": "Prayer of Healing", "ability_school": "holy"},
        ]
        
        self._boss_abilities = [
            {"ability_id": "boss001", "ability_name": "Lava Burst", "ability_school": "fire"},
            {"ability_id": "boss002", "ability_name": "Shadow Strike", "ability_school": "shadow"},
            {"ability_id": "boss003", "ability_name": "AOE Flame Wave", "ability_school": "fire"},
        ]

        # Clases por rol (strings para compatibilidad NumPy)
        self._classes_by_role = {
            "tank": ["warrior", "paladin", "death_knight"],
            "healer": ["priest", "paladin", "druid", "monk"],
            "dps": ["mage", "warlock", "hunter", "rogue", "shaman"],
        }

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
        
        # Generar jugadores
        players: List[Player] = []
        role_probs = [0.08, 0.20, 0.72]  # tank, healer, dps
        role_names = ["tank", "healer", "dps"]
        
        for i in range(num_players):
            # NumPy choice con strings (no Enums)
            role = str(self._rng.choice(role_names, p=role_probs))
            pclass = self._py_rng.choice(self._classes_by_role[role])
            name = f"Player_{i:02d}"
            player_id = f"player_{uuid.uuid4().hex[:8]}"
            
            players.append(Player(
                player_id=player_id,
                name=name,
                role=role,
                player_class=pclass,
                level=90,
                current_health_pct=100.0,
                current_mana_pct=100.0 if role in ["healer", "dps"] else 0.0,
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
        
        # Generar eventos por fase
        cumulative_time = 0
        for phase in session.phases:
            phase_start_ts = session.start_time.timestamp() + cumulative_time
            phase_end_ts = phase_start_ts + phase.duration_s
            
            # Eventos proporcionales a duración de fase
            total_duration = (session.end_time - session.start_time).total_seconds()
            phase_event_count = int(num_events * (phase.duration_s / total_duration))
            
            # Timestamps con burst patterns
            timestamps = self._generate_realistic_timestamps(
                start_ts=phase_start_ts,
                end_ts=phase_end_ts,
                count=phase_event_count,
                burst_intensity=0.6 if phase.phase_number >= 2 else 0.3
            )
            
            # Boss phase event al inicio
            if events or phase.phase_number > 1:
                phase_event = self._create_boss_phase_event(
                    session=session,
                    timestamp=datetime.fromtimestamp(phase_start_ts, tz=timezone.utc),
                    phase=phase
                )
                events.append(phase_event)
            
            # Generar eventos mixtos
            for ts in timestamps:
                timestamp = datetime.fromtimestamp(float(ts), tz=timezone.utc)
                
                # Seleccionar tipo por probabilidad
                event_type_rand = self._rng.random()
                cumulative_prob = 0.0
                
                for etype, prob in event_distribution.items():
                    cumulative_prob += prob
                    if event_type_rand <= cumulative_prob:
                        if etype == "damage":
                            ev = self._create_damage_event(session, timestamp, phase)
                        elif etype == "heal":
                            ev = self._create_heal_event(session, timestamp, phase)
                        elif etype == "spell_cast":
                            ev = self._create_spell_cast_event(session, timestamp, phase)
                        elif etype == "mana_regen":
                            ev = self._create_mana_regen_event(session, timestamp)
                        elif etype == "player_death":
                            ev = self._create_player_death_event(session, timestamp, phase)
                        else:
                            continue
                        
                        events.append(ev)
                        break
            
            cumulative_time += phase.duration_s
        
        # Ordenar por timestamp
        events.sort(key=lambda e: e.timestamp)
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

    # ===== CREADORES DE EVENTOS =====

    def _create_damage_event(
        self, 
        session: RaidSession, 
        timestamp: datetime, 
        phase: BossPhase
    ) -> WoWRaidEvent:
        """Genera evento de daño."""
        source = self._pick_player(session, role="dps")
        ability = self._py_rng.choice(self._damage_abilities)
        
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
        session: RaidSession, 
        timestamp: datetime, 
        phase: BossPhase
    ) -> WoWRaidEvent:
        """Genera evento de curación."""
        healer = self._pick_player(session, role="healer")
        target = self._py_rng.choice(session.players)
        ability = self._py_rng.choice(self._heal_abilities)
        
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
        session: RaidSession, 
        timestamp: datetime, 
        phase: BossPhase
    ) -> WoWRaidEvent:
        """Genera evento de spell_cast."""
        caster = self._pick_player(session)
        ability = self._py_rng.choice(self._damage_abilities + self._heal_abilities)
        
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
        session: RaidSession, 
        timestamp: datetime
    ) -> WoWRaidEvent:
        """Genera evento de regeneración de maná."""
        caster = self._pick_player(session)
        
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
            resource_type=ResourceType.MANA,
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
        session: RaidSession, 
        timestamp: datetime, 
        phase: BossPhase
    ) -> WoWRaidEvent:
        """Genera evento de muerte de jugador."""
        # Solo generar si probabilidad de fase lo permite
        if self._rng.random() > phase.death_probability:
            return self._create_heal_event(session, timestamp, phase)
        
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
        phase: BossPhase
    ) -> WoWRaidEvent:
        """Genera evento de transición de fase del boss."""
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
            target_entity_health_pct_before=float(100 - (phase.phase_number - 1) * 33),
            target_entity_health_pct_after=float(100 - phase.phase_number * 33),
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
