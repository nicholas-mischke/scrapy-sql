
from sqlalchemy import event
from sqlalchemy.orm import Session


def my_before_attach():
    input('Hello, World!')

# event.listen(Session, "before_attach", my_before_attach)


class UOWSession(Session):
    pass
