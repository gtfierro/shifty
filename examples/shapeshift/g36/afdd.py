"""Guideline 36 section 5: automatic fault detection and diagnostics (AFDD).

``afdd`` defines each point the fault conditions read, once, as the
guideline's point tables do. Each section's module follows: one shape per
fault condition.
"""

from __future__ import annotations

from brick import BRICK, brick, parts, points

from shifty.experimental import shapeshift as ss
from shifty.experimental.shapeshift import Shape, row

afdd = ss.Module(
    "afdd",
    base="urn:ashrae/g36/afdd/",
    comment="""
        ASHRAE Guideline 36-2021, section 5: the points the AFDD fault conditions
        read, each defined once, as the guideline's point tables do. A fault
        condition lists the ones it needs, and each row is named after its point:

          points required { SAT_AVG; MAT_AVG }

        In BuildingMOTIF's AFDD files every fault condition repeats each point's
        class and description.
    """,
)
afdd.uses(brick)

# Coil temperatures may come from the air temperature sensors.
_CCET = "Depending on the AHU configuration, this could be the MAT or a separate sensor for this specific purpose."
_CCLT = "Depending on the AHU configuration, this could be the SAT or a separate sensor for this specific purpose."

# heading: {point: (class, description)}
POINTS = {
    "Temperatures": {
        "SAT_AVG": (BRICK.Supply_Air_Temperature_Sensor, "rolling average of supply air temperature"),
        "MAT_AVG": (BRICK.Mixed_Air_Temperature_Sensor, "rolling average of mixed air temperature"),
        "RAT_AVG": (BRICK.Return_Air_Temperature_Sensor, "rolling average of return air temperature"),
        "OAT_AVG": (BRICK.Outside_Air_Temperature_Sensor, "rolling average of outdoor air temperature"),
        "MAT": (BRICK.Mixed_Air_Temperature_Sensor, "mixed air temperature"),
        "RAT": (BRICK.Return_Air_Temperature_Sensor, "return air temperature"),
        "OAT": (BRICK.Outside_Air_Temperature_Sensor, "outdoor air temperature"),
        "CCET_AVG": (BRICK.Mixed_Air_Temperature_Sensor, f"rolling average of cooling-coil entering temperature ({_CCET})"),
        "CCLT_AVG": (BRICK.Supply_Air_Temperature_Sensor, f"rolling average of cooling-coil leaving temperature ({_CCLT})"),
        "HCET_AVG": (BRICK.Mixed_Air_Temperature_Sensor, f"rolling average of heating-coil entering temperature ({_CCET})"),
        "HCLT_AVG": (BRICK.Supply_Air_Temperature_Sensor, f"rolling average of heating-coil leaving temperature ({_CCLT})"),
    },
    "Setpoints": {
        "SATSP": (BRICK.Supply_Air_Temperature_Setpoint, "supply air temperature setpoint"),
        "SATsp-C": (BRICK.Supply_Air_Temperature_Cooling_Setpoint, "supply air temperature setpoint for cooling coil control"),
        "DSPSP": (BRICK.Duct_Air_Static_Pressure_Setpoint, "duct static pressure setpoint"),
        "MinOAsp": (BRICK.Outside_Air_Flow_Setpoint, "minimum OA setpoint"),
    },
    "Pressure, fans, and modes": {
        "DSP": (BRICK.Static_Pressure_Sensor, "duct static pressure"),
        "DSP_AVG": (BRICK.Static_Pressure_Sensor, "rolling average of duct static pressure"),
        "VFDSPD": (BRICK.Frequency_Sensor, "VFD speed"),
        "Delta_OS": (BRICK.Operating_Mode_Status, "number of changes in OS during the previous 60 minutes (moving window)"),
    },
    "Coil valves": {
        "HC": (BRICK.Valve_Position_Command, "heating-coil valve position command; 0% <= HC <= 100%"),
        "CC": (BRICK.Valve_Position_Command, "cooling-coil valve position command; 0% <= CC <= 100%"),
    },
}  # fmt: skip

for heading, table in POINTS.items():
    for i, (name, (cls, description)) in enumerate(table.items()):
        heading_line = f"{heading} ".ljust(77, "-") if i == 0 else None
        afdd[name] = Shape(cls, doc=description, comment=heading_line)

# Many fault conditions need a coil's valve command.
afdd.hot_water_coil = Shape(
    parts.required(hw_coil=BRICK.Hot_Water_Coil & points.has(afdd.HC)),
    doc="A hot-water coil, with its valve command.",
    comment="Coils ------------------------------------------------------------------",
)
afdd.chilled_water_coil = Shape(
    parts.required(chw_coil=BRICK.Chilled_Water_Coil & points.has(afdd.CC)),
    doc="A chilled-water coil, with its valve command.",
)

# --- 5.16.14 ---------------------------------------------------------------------

mz = ss.Module(
    "5.16.14-multiple-zone-vav-ahu-afdd",
    base="urn:ashrae/g36/5.16.14/multiple-zone-vav-ahu-afdd/",
    comment="""
        ASHRAE Guideline 36-2021, 5.16.14: AFDD for multiple-zone VAV air handling units.
        Translated from BuildingMOTIF's
        libraries/ashrae/guideline36/5.16.14-multiple-zone-vav-ahu-afdd.ttl.
    """,
)
mz.uses(brick)
mz.uses(afdd)

mz.fc_1 = Shape(
    BRICK.Air_Handling_Unit,
    points.required(afdd.DSP_AVG, afdd.DSPSP),
    parts.required(
        vfd=row(
            BRICK.Fan & points.required(afdd.VFDSPD),
            comment="TODO only applies to supply fan",
        )
    ),
    doc="FC#1: Duct static pressure is too low with fan at full speed",
)
mz.fc_2 = Shape(
    BRICK.Air_Handling_Unit,
    points.required(afdd.MAT_AVG, afdd.RAT_AVG, afdd.OAT_AVG),
    doc="FC#2: MAT too low; should be between OAT and RAT",
)
mz.fc_3 = Shape(mz.fc_2, doc="FC#3: MAT too high; should be between OAT and RAT")
mz.fc_4 = Shape(
    BRICK.Air_Handling_Unit,
    points.required(afdd.Delta_OS),
    doc="FC#4: Too many changes in Operating State",
)
mz.fc_5 = Shape(
    BRICK.Air_Handling_Unit,
    points.required(afdd.SAT_AVG, afdd.MAT_AVG),
    doc="FC#5: SAT too low; should be higher than MAT",
)
mz.fc_6 = Shape(
    BRICK.Air_Handling_Unit,
    points.required(
        afdd.RAT_AVG,
        afdd.OAT_AVG,
        afdd.MinOAsp,
        row(
            BRICK.Outside_Air_Flow_Sensor,
            doc="actual total airflow",
            comment="NOTE unclear whether this is total OA or total supply air",
        ),
    ).comment(
        "%OAmin: active minimum OA setpoint (MinOAsp) divided by actual total airflow"
    ),
    (
        points.required(afdd.MAT, afdd.RAT, afdd.OAT)
        | points.required(
            row(
                BRICK.Outside_Air_Flow_Sensor,
                doc="actual outdoor air fraction per airflow measurement station if available.",
            )
        )
    ).comment("%OA: (MAT - RAT)/(OAT - RAT), or measured by an airflow station"),
    doc="FC#6: OA fraction is too low or too high; should equal %OA_MIN",
)
mz.fc_7 = Shape(
    BRICK.Air_Handling_Unit,
    points.required(afdd.SAT_AVG, afdd.SATSP),
    afdd.hot_water_coil,
    doc="FC#7: SAT too low in full heating",
)
mz.fc_8 = Shape(
    BRICK.Air_Handling_Unit,
    points.required(afdd.SAT_AVG, afdd.MAT_AVG),
    doc="FC#8: SAT and MAT should be approximately equal",
)
mz.fc_9 = Shape(
    BRICK.Air_Handling_Unit,
    points.required(afdd.OAT_AVG, afdd.SATSP),
    doc="FC#9: OAT is too high for free cooling without additional mechanical cooling",
)
mz.fc_10 = Shape(
    BRICK.Air_Handling_Unit,
    points.required(afdd.MAT_AVG, afdd.OAT_AVG),
    doc="FC#10: OAT and MAT should be approximately equal",
)
mz.fc_11 = Shape(
    BRICK.Air_Handling_Unit,
    points.required(afdd.OAT_AVG, afdd.SATSP),
    doc="FC#11: OAT is too low for mechanical cooling",
)
mz.fc_12 = Shape(
    BRICK.Air_Handling_Unit,
    points.required(afdd.SAT_AVG, afdd.MAT_AVG),
    doc="FC#12: SAT too high; should be less than MAT",
)
mz.fc_13 = Shape(
    BRICK.Air_Handling_Unit,
    points.required(afdd.SAT_AVG, afdd.SATSP),
    afdd.chilled_water_coil,
    doc="FC#13: SAT too high in full cooling",
)
mz.fc_14 = Shape(
    BRICK.Air_Handling_Unit,
    points.required(afdd.CCET_AVG, afdd.CCLT_AVG),
    doc="FC#14: Temperature drop across inactive cooling coil",
)
mz.fc_15 = Shape(
    BRICK.Air_Handling_Unit,
    points.required(afdd.HCLT_AVG, afdd.HCET_AVG),
    doc="FC#15: Temperature rise across inactive heating coil",
)

# --- 5.17.4 ----------------------------------------------------------------------

dual_fan = ss.Module(
    "5.17.4-dual-fan-dual-duct-heating-vav-ahu-afdd",
    base="urn:ashrae/g36/5.17.4/dual-fan-dual-duct-heating-vav-ahu-afdd/",
    comment="""
        ASHRAE Guideline 36-2021, 5.17.4: AFDD for dual-fan dual-duct heating VAV air handling units.
        Translated from BuildingMOTIF's
        libraries/ashrae/guideline36/5.17.4-dual-fan-dual-duct-heating-vav-ahu-afdd.ttl.
        FC#1 is the intended requirement, as in 5.16.14: the library puts
        `sh:qualifiedMinCount 1` inside the value shape, where SHACL ignores it.
    """,
)
dual_fan.uses(brick)
dual_fan.uses(afdd)

dual_fan.fc_1 = Shape(
    BRICK.Air_Handling_Unit,
    points.required(afdd.DSP, afdd.DSPSP),
    parts.required(
        vfd=row(
            BRICK.Fan & points.required(afdd.VFDSPD),
            comment="TODO only applies to supply fan",
        )
    ),
    doc="FC#1: Duct static pressure is too low with fan at full speed",
)
dual_fan.fc_2 = Shape(
    BRICK.Air_Handling_Unit,
    points.required(afdd.SAT_AVG, afdd.SATSP),
    afdd.hot_water_coil,
    doc="FC#2: SAT too low in full heating",
)
dual_fan.fc_3 = Shape(
    BRICK.Air_Handling_Unit,
    points.required(afdd.RAT_AVG, afdd.SAT_AVG),
    afdd.hot_water_coil,
    doc="FC#3: Temperature rise across inactive heating coil",
)

# --- 5.18.13 ---------------------------------------------------------------------
# A single-zone AHU shares all but FC#1, FC#6, and FC#13 with 5.16.14.

sz = ss.Module(
    "5.18.13-single-zone-vav-ahu-afdd",
    base="urn:ashrae/g36/5.18.13/single-zone-vav-ahu-afdd/",
    comment="""
        ASHRAE Guideline 36-2021, 5.18.13: AFDD for single-zone VAV air handling units.
        Translated from BuildingMOTIF's
        libraries/ashrae/guideline36/5.18.13-single-zone-vav-ahu-afdd.ttl.
    """,
)
sz.uses(brick)
sz.uses(afdd)
sz.uses(mz, alias="mz")

sz.fc_1 = Shape(
    BRICK.Air_Handling_Unit,
    doc=(
        "FC#1: This fault condition is not used in single zone units, "
        "as it requires a static pressure setpoint."
    ),
)
sz.fc_2 = Shape(mz.fc_2, doc="FC#2: MAT too low; should be between OAT and RAT")
sz.fc_3 = Shape(mz.fc_3, doc="FC#3: MAT too high; should be between OAT and RAT")
sz.fc_4 = Shape(mz.fc_4, doc="FC#4: Too many changes in Operating State")
sz.fc_5 = Shape(mz.fc_5, doc="FC#5: SAT too low; should be higher than MAT")
sz.fc_6 = Shape(
    BRICK.Air_Handling_Unit,
    points.required(afdd.RAT_AVG, afdd.OAT_AVG, afdd.MAT_AVG),
    doc="FC#6: OA fraction is too high; MAT should be closer to RAT than to OAT",
)
sz.fc_7 = Shape(mz.fc_7, doc="FC#7: SAT too low in full heating")
sz.fc_8 = Shape(mz.fc_8, doc="FC#8: SAT and MAT should be approximately equal")
sz.fc_9 = Shape(
    mz.fc_9,
    doc="FC#9: OAT is too high for free cooling without additional mechanical cooling",
)
sz.fc_10 = Shape(mz.fc_10, doc="FC#10: OAT and MAT should be approximately equal")
sz.fc_11 = Shape(mz.fc_11, doc="FC#11: OAT is too low for mechanical cooling")
sz.fc_12 = Shape(mz.fc_12, doc="FC#12: SAT too high; should be less than MAT")
sz.fc_13 = Shape(
    BRICK.Air_Handling_Unit,
    points.required(afdd.SAT_AVG, afdd["SATsp-C"]),
    afdd.chilled_water_coil,
    doc="FC#13: SAT too high in full cooling",
)
sz.fc_14 = Shape(mz.fc_14, doc="FC#14: Temperature drop across inactive cooling coil")
sz.fc_15 = Shape(mz.fc_15, doc="FC#15: Temperature rise across inactive heating coil")

# --- 5.22.6 ----------------------------------------------------------------------

fcu = ss.Module(
    "5.22.6-fan-coil-unit-afdd",
    base="urn:ashrae/g36/5.22.6/fan-coil-unit-afdd/",
    comment="""
        ASHRAE Guideline 36-2021, 5.22.6: AFDD for fan coil units.
        Translated from BuildingMOTIF's
        libraries/ashrae/guideline36/5.22.6-fan-coil-unit-afdd.ttl.
    """,
)
fcu.uses(brick)
fcu.uses(afdd)

fcu.fc_1 = Shape(
    BRICK.Air_Handling_Unit,
    points.required(afdd.Delta_OS),
    doc="FC#1: Too many changes in Operating State",
)
fcu.fc_2 = Shape(
    BRICK.Air_Handling_Unit,
    points.required(afdd.SAT_AVG, afdd.SATSP),
    afdd.hot_water_coil,
    doc="FC#2: SAT too low in full heating",
)
fcu.fc_3 = Shape(
    BRICK.Air_Handling_Unit,
    points.required(afdd.SAT_AVG, afdd.SATSP),
    afdd.chilled_water_coil,
    doc="FC#3: SAT too high in full cooling",
)
fcu.fc_4 = Shape(
    BRICK.Air_Handling_Unit,
    points.required(
        afdd.SAT_AVG, row(afdd.RAT, doc="return air temperature (if present)")
    ),
    doc="FC#4: Temperature drop across inactive cooling coil",
)
fcu.fc_5 = Shape(
    BRICK.Air_Handling_Unit,
    points.required(
        afdd.SAT_AVG, row(afdd.RAT, doc="return air temperature (if present)")
    ),
    doc="FC#5: Temperature rise across inactive heating coil",
)
