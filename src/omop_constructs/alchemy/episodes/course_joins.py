import sqlalchemy as sa
from .fraction_mv import FractionMV


from .treatment_inputs import treatment_detail

frac_summary_join = (
    sa.select(
        FractionMV.person_id,
        FractionMV.fraction_id,
        FractionMV.fraction_number,
        FractionMV.course_id,
        FractionMV.fraction_name,
        sa.func.min(FractionMV.procedure_datetime).label('first_exposure_date'),
        sa.func.max(FractionMV.procedure_datetime).label('last_exposure_date'),
        sa.func.count(sa.distinct(FractionMV.procedure_occurrence_id)).label('fraction_count'),
    )
    .group_by(FractionMV.person_id, FractionMV.fraction_id, FractionMV.fraction_number, FractionMV.course_id, FractionMV.fraction_name)
    .subquery()
)

course_join = treatment_detail(frac_summary_join, "course")
