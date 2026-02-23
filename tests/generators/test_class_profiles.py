# tests/test_class_profiles.py
import numpy as np
from src.generators.class_profiles import SPEC_PROFILES, SPECS_BY_ROLE

def test_known_role_assignments():
    """
    Valida cada (class, spec) → role contra el ground truth de MoP.
    Un fallo aquí indica una asignación incorrecta en SPEC_PROFILES.
    """
    MOP_GROUND_TRUTH: dict[tuple[str, str], str] = {
        ("death_knight", "blood"):      "tank",
        ("death_knight", "frost"):      "dps",
        ("death_knight", "unholy"):     "dps",
        ("druid", "balance"):           "dps",
        ("druid", "feral"):             "dps",
        ("druid", "guardian"):          "tank",
        ("druid", "restoration"):       "healer",
        ("hunter", "beast_mastery"):    "dps",
        ("hunter", "marksmanship"):     "dps",
        ("hunter", "survival"):         "dps",
        ("mage", "arcane"):             "dps",
        ("mage", "fire"):               "dps",
        ("mage", "frost"):              "dps",
        ("monk", "brewmaster"):         "tank",
        ("monk", "mistweaver"):         "healer",
        ("monk", "windwalker"):         "dps",
        ("paladin", "holy"):            "healer",
        ("paladin", "protection"):      "tank",
        ("paladin", "retribution"):     "dps",
        ("priest", "discipline"):       "healer",
        ("priest", "holy"):             "healer",
        ("priest", "shadow"):           "dps",
        ("rogue", "assassination"):     "dps",
        ("rogue", "combat"):            "dps",
        ("rogue", "subtlety"):          "dps",
        ("shaman", "elemental"):        "dps",
        ("shaman", "enhancement"):      "dps",
        ("shaman", "restoration"):      "healer",
        ("warlock", "affliction"):      "dps",
        ("warlock", "demonology"):      "dps",
        ("warlock", "destruction"):     "dps",
        ("warrior", "arms"):            "dps",
        ("warrior", "fury"):            "dps",
        ("warrior", "protection"):      "tank",
    }

    errors = []
    for (cls, spec), expected_role in MOP_GROUND_TRUTH.items():
        actual = SPEC_PROFILES[(cls, spec)]["role"]
        if actual != expected_role:
            errors.append(
                f"  ❌ ({cls}, {spec}): esperado '{expected_role}', tiene '{actual}'"
            )

    if errors:
        print(f"\n{len(errors)} asignaciones incorrectas:")
        for e in errors:
            print(e)
        raise AssertionError(f"{len(errors)} role assignments incorrectas en SPEC_PROFILES")

    print("✅ 34 role assignments validados contra MoP ground truth")


def test_weights_sum_to_one():
    for (cls, spec), profile in SPEC_PROFILES.items():
        total = sum(profile["event_weights"].values())
        assert abs(total - 1.0) < 1e-9, \
            f"❌ ({cls}, {spec}) suma {total:.10f}, esperado 1.0"
    print(f"✅ {len(SPEC_PROFILES)} specs validadas — todos los pesos suman 1.0")

def test_role_coverage():
    assert len(SPECS_BY_ROLE["tank"])   == 5,  f"Tanks: {len(SPECS_BY_ROLE['tank'])}"
    assert len(SPECS_BY_ROLE["healer"]) == 6,  f"Healers: {len(SPECS_BY_ROLE['healer'])}"
    assert len(SPECS_BY_ROLE["dps"])    == 23, f"DPS: {len(SPECS_BY_ROLE['dps'])}"
    print("✅ Composición de roles correcta: 5 tanks / 6 healers / 23 DPS")

def test_no_healer_abilities_in_pure_dps():
    for (cls, spec), profile in SPEC_PROFILES.items():
        if profile["role"] == "dps" and profile["event_weights"]["heal"] == 0.0:
            assert profile["abilities"]["heal"] == [], \
                f"❌ ({cls}, {spec}) tiene heal abilities pero weight=0"
    print("✅ Coherencia heal_abilities / heal_weight verificada")