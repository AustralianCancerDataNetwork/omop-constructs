from ..._lazy_imports import load_export

_EXPORTS: dict[str, str] = {
    "WeightDxMV": "omop_constructs.alchemy.events.dx_linked_event_mv",
    "WeightChangeDxMV": "omop_constructs.alchemy.events.dx_linked_event_mv",
    "HeightDxMV": "omop_constructs.alchemy.events.dx_linked_event_mv",
    "BSADxMV": "omop_constructs.alchemy.events.dx_linked_event_mv",
    "CreatinineClearanceDxMV": "omop_constructs.alchemy.events.dx_linked_event_mv",
    "EGFRDxMV": "omop_constructs.alchemy.events.dx_linked_event_mv",
    "FEV1DxMV": "omop_constructs.alchemy.events.dx_linked_event_mv",
    "DistressThermometerDxMV": "omop_constructs.alchemy.events.dx_linked_event_mv",
    "ECOGDxMV": "omop_constructs.alchemy.events.dx_linked_event_mv",
    "SmokingPYHDxMV": "omop_constructs.alchemy.events.dx_linked_event_mv",
    "DxMeasurementMV": "omop_constructs.alchemy.events.dx_linked_event_mv",
    "DxProcedureMV": "omop_constructs.alchemy.events.dx_linked_procedure_mv",
    "DxObservationMV": "omop_constructs.alchemy.events.dx_linked_obs_mv",
    "DxRelevantVisitMV": "omop_constructs.alchemy.events.dx_linked_visit_mv",
}

__all__ = [
    "WeightDxMV",
    "WeightChangeDxMV",         
    "HeightDxMV",
    "BSADxMV",
    "CreatinineClearanceDxMV",
    "EGFRDxMV",
    "FEV1DxMV",
    "DistressThermometerDxMV",
    "ECOGDxMV",
    "SmokingPYHDxMV",
    "DxMeasurementMV",
    "DxProcedureMV",
    "DxObservationMV",
    "DxRelevantVisitMV",
]


def __getattr__(name: str):
    return load_export(name, _EXPORTS)


def __dir__() -> list[str]:
    return sorted(__all__)
