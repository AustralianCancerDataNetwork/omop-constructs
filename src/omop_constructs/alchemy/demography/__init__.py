from ..._lazy_imports import load_export

_EXPORTS: dict[str, str] = {
    "PersonDemography": "omop_constructs.alchemy.demography.demography_matview",
}

__all__ = ["PersonDemography"]


def __getattr__(name: str):
    return load_export(name, _EXPORTS)


def __dir__() -> list[str]:
    return sorted(__all__)
