class ConstructError(Exception):
    """Base exception for omop_constructs."""


class DependencyCycleError(ConstructError):
    pass


class ConstructSpecError(ConstructError):
    pass
