
from orm_loader.helpers import Base
from .condition_joins import condition_window

class Condition_Window(Base):
    __table__ = condition_window
    __mapper_args__ = {
        "primary_key": [
            condition_window.c.person_id
        ]
    }