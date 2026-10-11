import sqlalchemy as sa
from .cycle_mv import CycleMV
from .treatment_inputs import treatment_detail


cycle_summary_join = (
    sa.select(
        CycleMV.person_id,
        CycleMV.cycle_id,
        CycleMV.cycle_number,
        CycleMV.regimen_id,
        CycleMV.cycle_concept,
        sa.func.min(CycleMV.drug_exposure_start_date).label('first_exposure_date'),
        sa.func.max(CycleMV.drug_exposure_end_date).label('last_exposure_date'),
        sa.func.count(sa.distinct(CycleMV.drug_exposure_id)).label('exposure_count'),
    )
    .group_by(CycleMV.person_id, CycleMV.cycle_id, CycleMV.cycle_number, CycleMV.regimen_id, CycleMV.cycle_concept)
    .subquery()
)

regimen_join = treatment_detail(cycle_summary_join, "regimen")
