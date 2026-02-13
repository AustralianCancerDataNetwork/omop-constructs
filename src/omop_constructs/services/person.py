import pandas as pd
import sqlalchemy as sa
import sqlalchemy.orm as so
from omop_alchemy.cdm.model.clinical import Person
from orm_loader.helpers import get_logger

logger = get_logger(__name__)

def get_existing_people(session: so.Session) -> pd.DataFrame:
    df = pd.DataFrame(
        session.execute(
            sa.select(
                Person.person_id,
                Person.person_source_value,
            )
        )
    )
    logger.info(f"{len(df)} existing people records")
    return df
