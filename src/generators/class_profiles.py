"""
MoP Class & Specialization Profiles
World of Warcraft: Mists of Pandaria — 11 classes, 34 specs

Estructura de cada entrada:
  (player_class, spec_name) → {
      role, dps_type, resource_type,
      abilities: {damage: [...], heal: [...]},
      event_weights: {combat_damage, spell_cast, heal, mana_regen, player_death}
  }

Invariante: sum(event_weights.values()) == 1.0 para toda spec.
"""

from __future__ import annotations

SPEC_PROFILES: dict[tuple[str, str], dict] = {

    # ================================================================
    # DEATH KNIGHT — resource: runic_power (+ rune system, no mana)
    # ================================================================
    ("death_knight", "blood"): {
        "role": "tank", "dps_type": None, "resource_type": "runic_power",
        "abilities": {
            "damage": [
                {"ability_id": "dk_b_001", "ability_name": "Death Strike",      "ability_school": "shadow"},
                {"ability_id": "dk_b_002", "ability_name": "Heart Strike",      "ability_school": "shadow"},
                {"ability_id": "dk_b_003", "ability_name": "Blood Boil",        "ability_school": "shadow"},
                {"ability_id": "dk_b_004", "ability_name": "Crimson Scourge",   "ability_school": "shadow"},
            ],
            "heal": [],
        },
        "event_weights": {"combat_damage": 0.48, "spell_cast": 0.33, "heal": 0.00, "mana_regen": 0.05, "player_death": 0.14},
    },
    ("death_knight", "frost"): {
        "role": "dps", "dps_type": "melee", "resource_type": "runic_power",
        "abilities": {
            "damage": [
                {"ability_id": "dk_f_001", "ability_name": "Frost Strike",   "ability_school": "frost"},
                {"ability_id": "dk_f_002", "ability_name": "Obliterate",     "ability_school": "physical"},
                {"ability_id": "dk_f_003", "ability_name": "Howling Blast",  "ability_school": "frost"},
                {"ability_id": "dk_f_004", "ability_name": "Pillar of Frost","ability_school": "frost"},
                {"ability_id": "dk_f_005", "ability_name": "Icy Touch",      "ability_school": "frost"},
            ],
            "heal": [],
        },
        "event_weights": {"combat_damage": 0.58, "spell_cast": 0.25, "heal": 0.00, "mana_regen": 0.05, "player_death": 0.12},
    },
    ("death_knight", "unholy"): {
        "role": "dps", "dps_type": "melee", "resource_type": "runic_power",
        "abilities": {
            "damage": [
                {"ability_id": "dk_u_001", "ability_name": "Scourge Strike",    "ability_school": "shadow"},
                {"ability_id": "dk_u_002", "ability_name": "Festering Strike",  "ability_school": "shadow"},
                {"ability_id": "dk_u_003", "ability_name": "Death Coil",        "ability_school": "shadow"},
                {"ability_id": "dk_u_004", "ability_name": "Dark Transformation","ability_school": "shadow"},
                {"ability_id": "dk_u_005", "ability_name": "Unholy Frenzy",     "ability_school": "shadow"},
            ],
            "heal": [],
        },
        "event_weights": {"combat_damage": 0.57, "spell_cast": 0.26, "heal": 0.00, "mana_regen": 0.05, "player_death": 0.12},
    },

    # ================================================================
    # DRUID — 4 specs (único con 4), recursos variados
    # ================================================================
    ("druid", "balance"): {
        "role": "dps", "dps_type": "caster", "resource_type": "mana",
        "abilities": {
            "damage": [
                {"ability_id": "drd_b_001", "ability_name": "Starsurge",  "ability_school": "arcane"},
                {"ability_id": "drd_b_002", "ability_name": "Starfall",   "ability_school": "arcane"},
                {"ability_id": "drd_b_003", "ability_name": "Moonfire",   "ability_school": "arcane"},
                {"ability_id": "drd_b_004", "ability_name": "Wrath",      "ability_school": "nature"},
                {"ability_id": "drd_b_005", "ability_name": "Sunfire",    "ability_school": "nature"},
            ],
            "heal": [],
        },
        "event_weights": {"combat_damage": 0.50, "spell_cast": 0.28, "heal": 0.00, "mana_regen": 0.15, "player_death": 0.07},
    },
    ("druid", "feral"): {
        "role": "dps", "dps_type": "melee", "resource_type": "energy",
        "abilities": {
            "damage": [
                {"ability_id": "drd_f_001", "ability_name": "Rake",          "ability_school": "physical"},
                {"ability_id": "drd_f_002", "ability_name": "Rip",           "ability_school": "physical"},
                {"ability_id": "drd_f_003", "ability_name": "Shred",         "ability_school": "physical"},
                {"ability_id": "drd_f_004", "ability_name": "Tiger's Fury",  "ability_school": "physical"},
                {"ability_id": "drd_f_005", "ability_name": "Ferocious Bite","ability_school": "physical"},
            ],
            "heal": [],
        },
        "event_weights": {"combat_damage": 0.63, "spell_cast": 0.22, "heal": 0.00, "mana_regen": 0.07, "player_death": 0.08},
    },
    ("druid", "guardian"): {
        "role": "tank", "dps_type": None, "resource_type": "rage",
        "abilities": {
            "damage": [
                {"ability_id": "drd_g_001", "ability_name": "Mangle",         "ability_school": "physical"},
                {"ability_id": "drd_g_002", "ability_name": "Thrash",         "ability_school": "physical"},
                {"ability_id": "drd_g_003", "ability_name": "Lacerate",       "ability_school": "physical"},
                {"ability_id": "drd_g_004", "ability_name": "Maul",           "ability_school": "physical"},
                {"ability_id": "drd_g_005", "ability_name": "Savage Defense", "ability_school": "physical"},
            ],
            "heal": [],
        },
        "event_weights": {"combat_damage": 0.50, "spell_cast": 0.32, "heal": 0.00, "mana_regen": 0.05, "player_death": 0.13},
    },
    ("druid", "restoration"): {
        "role": "healer", "dps_type": None, "resource_type": "mana",
        "abilities": {
            "damage": [
                {"ability_id": "drd_r_d01", "ability_name": "Moonfire",     "ability_school": "arcane"},
            ],
            "heal": [
                {"ability_id": "drd_r_001", "ability_name": "Rejuvenation", "ability_school": "nature"},
                {"ability_id": "drd_r_002", "ability_name": "Lifebloom",    "ability_school": "nature"},
                {"ability_id": "drd_r_003", "ability_name": "Wild Growth",  "ability_school": "nature"},
                {"ability_id": "drd_r_004", "ability_name": "Regrowth",     "ability_school": "nature"},
                {"ability_id": "drd_r_005", "ability_name": "Tranquility",  "ability_school": "nature"},
            ],
        },
        "event_weights": {"combat_damage": 0.02, "spell_cast": 0.13, "heal": 0.65, "mana_regen": 0.15, "player_death": 0.05},
    },

    # ================================================================
    # HUNTER — resource: focus (regenerativo, como energy pero más lento)
    # ================================================================
    ("hunter", "beast_mastery"): {
        "role": "dps", "dps_type": "ranged", "resource_type": "focus",
        "abilities": {
            "damage": [
                {"ability_id": "hnt_bm_001", "ability_name": "Kill Command",  "ability_school": "physical"},
                {"ability_id": "hnt_bm_002", "ability_name": "Cobra Shot",    "ability_school": "nature"},
                {"ability_id": "hnt_bm_003", "ability_name": "Arcane Shot",   "ability_school": "arcane"},
                {"ability_id": "hnt_bm_004", "ability_name": "Kill Shot",     "ability_school": "physical"},
                {"ability_id": "hnt_bm_005", "ability_name": "Bestial Wrath", "ability_school": "physical"},
            ],
            "heal": [],
        },
        "event_weights": {"combat_damage": 0.60, "spell_cast": 0.25, "heal": 0.00, "mana_regen": 0.09, "player_death": 0.06},
    },
    ("hunter", "marksmanship"): {
        "role": "dps", "dps_type": "ranged", "resource_type": "focus",
        "abilities": {
            "damage": [
                {"ability_id": "hnt_mm_001", "ability_name": "Aimed Shot",   "ability_school": "physical"},
                {"ability_id": "hnt_mm_002", "ability_name": "Chimera Shot", "ability_school": "nature"},
                {"ability_id": "hnt_mm_003", "ability_name": "Steady Shot",  "ability_school": "physical"},
                {"ability_id": "hnt_mm_004", "ability_name": "Kill Shot",    "ability_school": "physical"},
                {"ability_id": "hnt_mm_005", "ability_name": "Rapid Fire",   "ability_school": "physical"},
            ],
            "heal": [],
        },
        "event_weights": {"combat_damage": 0.62, "spell_cast": 0.23, "heal": 0.00, "mana_regen": 0.09, "player_death": 0.06},
    },
    ("hunter", "survival"): {
        "role": "dps", "dps_type": "ranged", "resource_type": "focus",
        "abilities": {
            "damage": [
                {"ability_id": "hnt_sv_001", "ability_name": "Explosive Shot","ability_school": "fire"},
                {"ability_id": "hnt_sv_002", "ability_name": "Black Arrow",   "ability_school": "shadow"},
                {"ability_id": "hnt_sv_003", "ability_name": "Arcane Shot",   "ability_school": "arcane"},
                {"ability_id": "hnt_sv_004", "ability_name": "Serpent Sting", "ability_school": "nature"},
                {"ability_id": "hnt_sv_005", "ability_name": "Kill Shot",     "ability_school": "physical"},
            ],
            "heal": [],
        },
        "event_weights": {"combat_damage": 0.60, "spell_cast": 0.25, "heal": 0.00, "mana_regen": 0.09, "player_death": 0.06},
    },

    # ================================================================
    # MAGE — resource: mana. 3 specs, 3 escuelas distintas (signal diversity)
    # ================================================================
    ("mage", "arcane"): {
        "role": "dps", "dps_type": "caster", "resource_type": "mana",
        "abilities": {
            "damage": [
                {"ability_id": "mage_a_001", "ability_name": "Arcane Blast",    "ability_school": "arcane"},
                {"ability_id": "mage_a_002", "ability_name": "Arcane Barrage",  "ability_school": "arcane"},
                {"ability_id": "mage_a_003", "ability_name": "Arcane Missiles", "ability_school": "arcane"},
                {"ability_id": "mage_a_004", "ability_name": "Arcane Power",    "ability_school": "arcane"},
            ],
            "heal": [],
        },
        "event_weights": {"combat_damage": 0.52, "spell_cast": 0.28, "heal": 0.00, "mana_regen": 0.14, "player_death": 0.06},
    },
    ("mage", "fire"): {
        "role": "dps", "dps_type": "caster", "resource_type": "mana",
        "abilities": {
            "damage": [
                {"ability_id": "mage_f_001", "ability_name": "Fireball",    "ability_school": "fire"},
                {"ability_id": "mage_f_002", "ability_name": "Pyroblast",   "ability_school": "fire"},
                {"ability_id": "mage_f_003", "ability_name": "Fire Blast",  "ability_school": "fire"},
                {"ability_id": "mage_f_004", "ability_name": "Combustion",  "ability_school": "fire"},
                {"ability_id": "mage_f_005", "ability_name": "Living Bomb", "ability_school": "fire"},
            ],
            "heal": [],
        },
        "event_weights": {"combat_damage": 0.54, "spell_cast": 0.28, "heal": 0.00, "mana_regen": 0.13, "player_death": 0.05},
    },
    ("mage", "frost"): {
        "role": "dps", "dps_type": "caster", "resource_type": "mana",
        "abilities": {
            "damage": [
                {"ability_id": "mage_fr_001", "ability_name": "Frostbolt",     "ability_school": "frost"},
                {"ability_id": "mage_fr_002", "ability_name": "Ice Lance",     "ability_school": "frost"},
                {"ability_id": "mage_fr_003", "ability_name": "Frozen Orb",    "ability_school": "frost"},
                {"ability_id": "mage_fr_004", "ability_name": "Deep Freeze",   "ability_school": "frost"},
                {"ability_id": "mage_fr_005", "ability_name": "Frostfire Bolt","ability_school": "frost"},
            ],
            "heal": [],
        },
        "event_weights": {"combat_damage": 0.52, "spell_cast": 0.29, "heal": 0.00, "mana_regen": 0.14, "player_death": 0.05},
    },

    # ================================================================
    # MONK — nueva clase MoP, resource: energy/chi (tanks+dps) y mana/chi (healer)
    # ================================================================
    ("monk", "brewmaster"): {
        "role": "tank", "dps_type": None, "resource_type": "energy",
        "abilities": {
            "damage": [
                {"ability_id": "mnk_bm_001", "ability_name": "Keg Smash",     "ability_school": "physical"},
                {"ability_id": "mnk_bm_002", "ability_name": "Blackout Kick", "ability_school": "physical"},
                {"ability_id": "mnk_bm_003", "ability_name": "Purifying Brew","ability_school": "nature"},
                {"ability_id": "mnk_bm_004", "ability_name": "Dizzying Haze", "ability_school": "nature"},
                {"ability_id": "mnk_bm_005", "ability_name": "Guard",         "ability_school": "nature"},
            ],
            "heal": [],
        },
        "event_weights": {"combat_damage": 0.48, "spell_cast": 0.33, "heal": 0.00, "mana_regen": 0.07, "player_death": 0.12},
    },
    ("monk", "mistweaver"): {
        "role": "healer", "dps_type": None, "resource_type": "mana",
        "abilities": {
            "damage": [
                {"ability_id": "mnk_mw_d01", "ability_name": "Tiger Palm",       "ability_school": "physical"},
            ],
            "heal": [
                {"ability_id": "mnk_mw_001", "ability_name": "Soothing Mist",    "ability_school": "nature"},
                {"ability_id": "mnk_mw_002", "ability_name": "Uplift",           "ability_school": "nature"},
                {"ability_id": "mnk_mw_003", "ability_name": "Enveloping Mist",  "ability_school": "nature"},
                {"ability_id": "mnk_mw_004", "ability_name": "Renewing Mist",    "ability_school": "nature"},
                {"ability_id": "mnk_mw_005", "ability_name": "Revival",          "ability_school": "nature"},
            ],
        },
        "event_weights": {"combat_damage": 0.03, "spell_cast": 0.15, "heal": 0.65, "mana_regen": 0.12, "player_death": 0.05},
    },
    ("monk", "windwalker"): {
        "role": "dps", "dps_type": "melee", "resource_type": "energy",
        "abilities": {
            "damage": [
                {"ability_id": "mnk_ww_001", "ability_name": "Tiger Palm",           "ability_school": "physical"},
                {"ability_id": "mnk_ww_002", "ability_name": "Blackout Kick",        "ability_school": "physical"},
                {"ability_id": "mnk_ww_003", "ability_name": "Rising Sun Kick",      "ability_school": "physical"},
                {"ability_id": "mnk_ww_004", "ability_name": "Fists of Fury",        "ability_school": "physical"},
                {"ability_id": "mnk_ww_005", "ability_name": "Storm, Earth and Fire","ability_school": "nature"},
            ],
            "heal": [],
        },
        "event_weights": {"combat_damage": 0.62, "spell_cast": 0.22, "heal": 0.00, "mana_regen": 0.09, "player_death": 0.07},
    },

    # ================================================================
    # PALADIN — dual-school (holy + physical). Único tank con mana.
    # ================================================================
    ("paladin", "holy"): {
        "role": "healer", "dps_type": None, "resource_type": "mana",
        "abilities": {
            "damage": [
                {"ability_id": "pal_h_d01", "ability_name": "Exorcism",       "ability_school": "holy"},
            ],
            "heal": [
                {"ability_id": "pal_h_001", "ability_name": "Holy Light",     "ability_school": "holy"},
                {"ability_id": "pal_h_002", "ability_name": "Flash of Light", "ability_school": "holy"},
                {"ability_id": "pal_h_003", "ability_name": "Divine Light",   "ability_school": "holy"},
                {"ability_id": "pal_h_004", "ability_name": "Holy Shock",     "ability_school": "holy"},
                {"ability_id": "pal_h_005", "ability_name": "Holy Radiance",  "ability_school": "holy"},
            ],
        },
        "event_weights": {"combat_damage": 0.02, "spell_cast": 0.15, "heal": 0.67, "mana_regen": 0.13, "player_death": 0.03},
    },
    ("paladin", "protection"): {
        "role": "tank", "dps_type": None, "resource_type": "mana",
        "abilities": {
            "damage": [
                {"ability_id": "pal_p_001", "ability_name": "Shield of the Righteous","ability_school": "holy"},
                {"ability_id": "pal_p_002", "ability_name": "Judgment",               "ability_school": "holy"},
                {"ability_id": "pal_p_003", "ability_name": "Avenger's Shield",       "ability_school": "holy"},
                {"ability_id": "pal_p_004", "ability_name": "Hammer of the Righteous","ability_school": "holy"},
                {"ability_id": "pal_p_005", "ability_name": "Consecration",           "ability_school": "holy"},
            ],
            "heal": [],
        },
        "event_weights": {"combat_damage": 0.45, "spell_cast": 0.32, "heal": 0.00, "mana_regen": 0.12, "player_death": 0.11},
    },
    ("paladin", "retribution"): {
        "role": "dps", "dps_type": "melee", "resource_type": "mana",
        "abilities": {
            "damage": [
                {"ability_id": "pal_r_001", "ability_name": "Templar's Verdict","ability_school": "holy"},
                {"ability_id": "pal_r_002", "ability_name": "Crusader Strike",  "ability_school": "physical"},
                {"ability_id": "pal_r_003", "ability_name": "Exorcism",         "ability_school": "holy"},
                {"ability_id": "pal_r_004", "ability_name": "Hammer of Wrath",  "ability_school": "holy"},
                {"ability_id": "pal_r_005", "ability_name": "Divine Storm",     "ability_school": "holy"},
            ],
            "heal": [],
        },
        "event_weights": {"combat_damage": 0.55, "spell_cast": 0.27, "heal": 0.00, "mana_regen": 0.12, "player_death": 0.06},
    },

    # ================================================================
    # PRIEST — único con dos specs de healer + 1 DPS caster puro
    # ================================================================
    ("priest", "discipline"): {
        "role": "healer", "dps_type": None, "resource_type": "mana",
        "abilities": {
            "damage": [
                {"ability_id": "prs_d_d01", "ability_name": "Smite",           "ability_school": "holy"},
                {"ability_id": "prs_d_d02", "ability_name": "Penance",         "ability_school": "holy"},
            ],
            "heal": [
                {"ability_id": "prs_d_001", "ability_name": "Power Word: Shield","ability_school": "holy"},
                {"ability_id": "prs_d_002", "ability_name": "Penance (heal)",   "ability_school": "holy"},
                {"ability_id": "prs_d_003", "ability_name": "Prayer of Healing","ability_school": "holy"},
                {"ability_id": "prs_d_004", "ability_name": "Spirit Shell",     "ability_school": "holy"},
                {"ability_id": "prs_d_005", "ability_name": "Atonement",        "ability_school": "holy"},
            ],
        },
        "event_weights": {"combat_damage": 0.05, "spell_cast": 0.15, "heal": 0.63, "mana_regen": 0.13, "player_death": 0.04},
    },
    ("priest", "holy"): {
        "role": "healer", "dps_type": None, "resource_type": "mana",
        "abilities": {
            "damage": [
                {"ability_id": "prs_h_d01", "ability_name": "Smite",            "ability_school": "holy"},
            ],
            "heal": [
                {"ability_id": "prs_h_001", "ability_name": "Serenity",          "ability_school": "holy"},
                {"ability_id": "prs_h_002", "ability_name": "Circle of Healing", "ability_school": "holy"},
                {"ability_id": "prs_h_003", "ability_name": "Prayer of Mending", "ability_school": "holy"},
                {"ability_id": "prs_h_004", "ability_name": "Binding Heal",      "ability_school": "holy"},
                {"ability_id": "prs_h_005", "ability_name": "Renew",             "ability_school": "holy"},
            ],
        },
        "event_weights": {"combat_damage": 0.02, "spell_cast": 0.13, "heal": 0.68, "mana_regen": 0.14, "player_death": 0.03},
    },
    ("priest", "shadow"): {
        "role": "dps", "dps_type": "caster", "resource_type": "mana",
        "abilities": {
            "damage": [
                {"ability_id": "prs_s_001", "ability_name": "Mind Blast",       "ability_school": "shadow"},
                {"ability_id": "prs_s_002", "ability_name": "Shadow Word: Pain","ability_school": "shadow"},
                {"ability_id": "prs_s_003", "ability_name": "Vampiric Touch",   "ability_school": "shadow"},
                {"ability_id": "prs_s_004", "ability_name": "Mind Flay",        "ability_school": "shadow"},
                {"ability_id": "prs_s_005", "ability_name": "Devouring Plague", "ability_school": "shadow"},
            ],
            "heal": [],
        },
        "event_weights": {"combat_damage": 0.52, "spell_cast": 0.28, "heal": 0.00, "mana_regen": 0.14, "player_death": 0.06},
    },

    # ================================================================
    # ROGUE — resource: energy (no mana_regen real, bajo peso)
    # ================================================================
    ("rogue", "assassination"): {
        "role": "dps", "dps_type": "melee", "resource_type": "energy",
        "abilities": {
            "damage": [
                {"ability_id": "rog_a_001", "ability_name": "Mutilate",  "ability_school": "physical"},
                {"ability_id": "rog_a_002", "ability_name": "Envenom",   "ability_school": "nature"},
                {"ability_id": "rog_a_003", "ability_name": "Rupture",   "ability_school": "physical"},
                {"ability_id": "rog_a_004", "ability_name": "Dispatch",  "ability_school": "physical"},
                {"ability_id": "rog_a_005", "ability_name": "Garrote",   "ability_school": "physical"},
            ],
            "heal": [],
        },
        "event_weights": {"combat_damage": 0.65, "spell_cast": 0.22, "heal": 0.00, "mana_regen": 0.06, "player_death": 0.07},
    },
    ("rogue", "combat"): {
        "role": "dps", "dps_type": "melee", "resource_type": "energy",
        "abilities": {
            "damage": [
                {"ability_id": "rog_c_001", "ability_name": "Sinister Strike", "ability_school": "physical"},
                {"ability_id": "rog_c_002", "ability_name": "Revealing Strike","ability_school": "physical"},
                {"ability_id": "rog_c_003", "ability_name": "Eviscerate",      "ability_school": "physical"},
                {"ability_id": "rog_c_004", "ability_name": "Blade Flurry",    "ability_school": "physical"},
                {"ability_id": "rog_c_005", "ability_name": "Adrenaline Rush", "ability_school": "physical"},
            ],
            "heal": [],
        },
        "event_weights": {"combat_damage": 0.66, "spell_cast": 0.20, "heal": 0.00, "mana_regen": 0.06, "player_death": 0.08},
    },
    ("rogue", "subtlety"): {
        "role": "dps", "dps_type": "melee", "resource_type": "energy",
        "abilities": {
            "damage": [
                {"ability_id": "rog_s_001", "ability_name": "Backstab",   "ability_school": "physical"},
                {"ability_id": "rog_s_002", "ability_name": "Hemorrhage", "ability_school": "shadow"},
                {"ability_id": "rog_s_003", "ability_name": "Ambush",     "ability_school": "physical"},
                {"ability_id": "rog_s_004", "ability_name": "Eviscerate", "ability_school": "physical"},
                {"ability_id": "rog_s_005", "ability_name": "Shadow Dance","ability_school": "shadow"},
            ],
            "heal": [],
        },
        "event_weights": {"combat_damage": 0.65, "spell_cast": 0.22, "heal": 0.00, "mana_regen": 0.06, "player_death": 0.07},
    },

    # ================================================================
    # SHAMAN — Enhancement es melee con mana (caso especial)
    # ================================================================
    ("shaman", "elemental"): {
        "role": "dps", "dps_type": "caster", "resource_type": "mana",
        "abilities": {
            "damage": [
                {"ability_id": "shm_e_001", "ability_name": "Lightning Bolt",  "ability_school": "nature"},
                {"ability_id": "shm_e_002", "ability_name": "Lava Burst",      "ability_school": "fire"},
                {"ability_id": "shm_e_003", "ability_name": "Chain Lightning", "ability_school": "nature"},
                {"ability_id": "shm_e_004", "ability_name": "Earth Shock",     "ability_school": "nature"},
                {"ability_id": "shm_e_005", "ability_name": "Flame Shock",     "ability_school": "fire"},
            ],
            "heal": [],
        },
        "event_weights": {"combat_damage": 0.50, "spell_cast": 0.28, "heal": 0.00, "mana_regen": 0.15, "player_death": 0.07},
    },
    ("shaman", "enhancement"): {
        "role": "dps", "dps_type": "melee", "resource_type": "mana",
        "abilities": {
            "damage": [
                {"ability_id": "shm_en_001", "ability_name": "Stormstrike",       "ability_school": "nature"},
                {"ability_id": "shm_en_002", "ability_name": "Lava Lash",         "ability_school": "fire"},
                {"ability_id": "shm_en_003", "ability_name": "Maelstrom Weapon",  "ability_school": "nature"},
                {"ability_id": "shm_en_004", "ability_name": "Unleash Elements",  "ability_school": "nature"},
                {"ability_id": "shm_en_005", "ability_name": "Earth Shock",       "ability_school": "nature"},
            ],
            "heal": [],
        },
        "event_weights": {"combat_damage": 0.58, "spell_cast": 0.25, "heal": 0.00, "mana_regen": 0.11, "player_death": 0.06},
    },
    ("shaman", "restoration"): {
        "role": "healer", "dps_type": None, "resource_type": "mana",
        "abilities": {
            "damage": [
                {"ability_id": "shm_r_d01", "ability_name": "Lightning Bolt",   "ability_school": "nature"},
            ],
            "heal": [
                {"ability_id": "shm_r_001", "ability_name": "Chain Heal",       "ability_school": "nature"},
                {"ability_id": "shm_r_002", "ability_name": "Healing Wave",     "ability_school": "nature"},
                {"ability_id": "shm_r_003", "ability_name": "Riptide",          "ability_school": "nature"},
                {"ability_id": "shm_r_004", "ability_name": "Healing Rain",     "ability_school": "nature"},
                {"ability_id": "shm_r_005", "ability_name": "Greater Healing Wave","ability_school": "nature"},
            ],
        },
        "event_weights": {"combat_damage": 0.02, "spell_cast": 0.13, "heal": 0.67, "mana_regen": 0.14, "player_death": 0.04},
    },

    # ================================================================
    # WARLOCK — 3 specs, 3 escuelas dominantes: shadow, chaos, fire
    # ================================================================
    ("warlock", "affliction"): {
        "role": "dps", "dps_type": "caster", "resource_type": "mana",
        "abilities": {
            "damage": [
                {"ability_id": "wlk_af_001", "ability_name": "Haunt",          "ability_school": "shadow"},
                {"ability_id": "wlk_af_002", "ability_name": "Corruption",     "ability_school": "shadow"},
                {"ability_id": "wlk_af_003", "ability_name": "Agony",          "ability_school": "shadow"},
                {"ability_id": "wlk_af_004", "ability_name": "Malefic Grasp",  "ability_school": "shadow"},
                {"ability_id": "wlk_af_005", "ability_name": "Drain Soul",     "ability_school": "shadow"},
            ],
            "heal": [],
        },
        "event_weights": {"combat_damage": 0.54, "spell_cast": 0.27, "heal": 0.00, "mana_regen": 0.13, "player_death": 0.06},
    },
    ("warlock", "demonology"): {
        "role": "dps", "dps_type": "caster", "resource_type": "mana",
        "abilities": {
            "damage": [
                {"ability_id": "wlk_dm_001", "ability_name": "Hand of Gul'dan", "ability_school": "shadow"},
                {"ability_id": "wlk_dm_002", "ability_name": "Soul Fire",       "ability_school": "fire"},
                {"ability_id": "wlk_dm_003", "ability_name": "Metamorphosis",   "ability_school": "chaos"},
                {"ability_id": "wlk_dm_004", "ability_name": "Fel Flame",       "ability_school": "fire"},
                {"ability_id": "wlk_dm_005", "ability_name": "Shadowbolt",      "ability_school": "shadow"},
            ],
            "heal": [],
        },
        "event_weights": {"combat_damage": 0.52, "spell_cast": 0.28, "heal": 0.00, "mana_regen": 0.14, "player_death": 0.06},
    },
    ("warlock", "destruction"): {
        "role": "dps", "dps_type": "caster", "resource_type": "mana",
        "abilities": {
            "damage": [
                {"ability_id": "wlk_ds_001", "ability_name": "Chaos Bolt",   "ability_school": "chaos"},
                {"ability_id": "wlk_ds_002", "ability_name": "Incinerate",   "ability_school": "fire"},
                {"ability_id": "wlk_ds_003", "ability_name": "Conflagrate",  "ability_school": "fire"},
                {"ability_id": "wlk_ds_004", "ability_name": "Havoc",        "ability_school": "fire"},
                {"ability_id": "wlk_ds_005", "ability_name": "Rain of Fire", "ability_school": "fire"},
            ],
            "heal": [],
        },
        "event_weights": {"combat_damage": 0.54, "spell_cast": 0.27, "heal": 0.00, "mana_regen": 0.13, "player_death": 0.06},
    },

    # ================================================================
    # WARRIOR — resource: rage (no mana, bajo mana_regen)
    # ================================================================
    ("warrior", "arms"): {
        "role": "dps", "dps_type": "melee", "resource_type": "rage",
        "abilities": {
            "damage": [
                {"ability_id": "war_a_001", "ability_name": "Mortal Strike",  "ability_school": "physical"},
                {"ability_id": "war_a_002", "ability_name": "Overpower",      "ability_school": "physical"},
                {"ability_id": "war_a_003", "ability_name": "Slam",           "ability_school": "physical"},
                {"ability_id": "war_a_004", "ability_name": "Colossus Smash", "ability_school": "physical"},
                {"ability_id": "war_a_005", "ability_name": "Execute",        "ability_school": "physical"},
            ],
            "heal": [],
        },
        "event_weights": {"combat_damage": 0.63, "spell_cast": 0.22, "heal": 0.00, "mana_regen": 0.04, "player_death": 0.11},
    },
    ("warrior", "fury"): {
        "role": "dps", "dps_type": "melee", "resource_type": "rage",
        "abilities": {
            "damage": [
                {"ability_id": "war_f_001", "ability_name": "Bloodthirst",    "ability_school": "physical"},
                {"ability_id": "war_f_002", "ability_name": "Raging Blow",    "ability_school": "physical"},
                {"ability_id": "war_f_003", "ability_name": "Wild Strike",    "ability_school": "physical"},
                {"ability_id": "war_f_004", "ability_name": "Execute",        "ability_school": "physical"},
                {"ability_id": "war_f_005", "ability_name": "Colossus Smash", "ability_school": "physical"},
            ],
            "heal": [],
        },
        "event_weights": {"combat_damage": 0.65, "spell_cast": 0.20, "heal": 0.00, "mana_regen": 0.04, "player_death": 0.11},
    },
    ("warrior", "protection"): {
        "role": "tank", "dps_type": None, "resource_type": "rage",
        "abilities": {
            "damage": [
                {"ability_id": "war_p_001", "ability_name": "Shield Slam",  "ability_school": "physical"},
                {"ability_id": "war_p_002", "ability_name": "Revenge",      "ability_school": "physical"},
                {"ability_id": "war_p_003", "ability_name": "Thunder Clap", "ability_school": "physical"},
                {"ability_id": "war_p_004", "ability_name": "Shield Block", "ability_school": "physical"},
                {"ability_id": "war_p_005", "ability_name": "Devastate",    "ability_school": "physical"},
            ],
            "heal": [],
        },
        "event_weights": {"combat_damage": 0.50, "spell_cast": 0.30, "heal": 0.00, "mana_regen": 0.05, "player_death": 0.15},
    },
}


# ================================================================
# ESTRUCTURAS DERIVADAS — construidas una sola vez al importar el módulo
# ================================================================

# Quick-lookup: (class, spec) → role
SPEC_TO_ROLE: dict[tuple[str, str], str] = {
    k: v["role"] for k, v in SPEC_PROFILES.items()
}

# Quick-lookup: (class, spec) → resource_type
SPEC_TO_RESOURCE: dict[tuple[str, str], str] = {
    k: v["resource_type"] for k, v in SPEC_PROFILES.items()
}

# Agrupado por rol — usado por WoWEventGenerator para generar composición de raid
SPECS_BY_ROLE: dict[str, list[tuple[str, str]]] = {
    "tank":   [k for k, v in SPEC_PROFILES.items() if v["role"] == "tank"],
    "healer": [k for k, v in SPEC_PROFILES.items() if v["role"] == "healer"],
    "dps":    [k for k, v in SPEC_PROFILES.items() if v["role"] == "dps"],
}

# Composición de raid realista MoP 20-25 jugadores
# Pesos para np.random.choice al generar jugadores
RAID_ROLE_WEIGHTS: dict[str, float] = {
    "tank":   0.10,
    "healer": 0.20,
    "dps":    0.70,
}
