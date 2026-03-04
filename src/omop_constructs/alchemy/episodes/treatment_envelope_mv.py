import sqlalchemy as sa
import sqlalchemy.orm as so
from orm_loader.helpers import Base
from datetime import date
from typing import Optional
from .treatment_envelope_query import treatment_envelope_with_scalars
from ...core.materialized import MaterializedViewMixin
from ...core.constructs import register_construct

from ..modifiers import ModifiedCondition
from .surgical_procedure_mv import SurgicalProcedureMV
from .systemic_treatment_mv import SACTRegimenMV
from .course_mv import RTCourseMV


@register_construct
class TreatmentEnvelopeMV(
    MaterializedViewMixin,
    Base,
):
    __mv_name__ = "treatment_envelope_mv"
    __mv_select__ = treatment_envelope_with_scalars.select()
    __mv_index__ = "mv_id"
    __deps__ = (
        ModifiedCondition.__mv_name__, 
        SurgicalProcedureMV.__mv_name__, 
        SACTRegimenMV.__mv_name__, 
        RTCourseMV.__mv_name__,
    )
    __tablename__ = __mv_name__
    __table_args__ = {"extend_existing": True}

    mv_id = sa.Column(primary_key=True)
    person_id = sa.Column(sa.Integer)
    condition_episode = sa.Column(sa.Integer)
    condition_start_date = sa.Column(sa.Date)
    earliest_treatment = sa.Column(sa.Date)
    latest_treatment = sa.Column(sa.Date)
    death_datetime = sa.Column(sa.DateTime)
    treatment_days_before_death = sa.Column(sa.Float)
    days_from_dx_to_treatment = sa.Column(sa.Float)
    concurrent_chemort = sa.Column(sa.Boolean)