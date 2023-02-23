
from sqlalchemy import Integer, sql
from sqlalchemy.orm import attributes


def get_filter_by_kwargs(instance):

    d = {}

    for column in instance.__table__.columns:

        value = getattr(instance, column.name)

        # Don't include NoneType when filtering
        if value is not None:
            d[column.name] = value

        # Don't include autoincrement columns when filtering
        if (
            column.autoincrement is True
            or (
                isinstance(column.type, Integer)
                and column.primary_key is True
                and column.foreign_keys == set()
            )
        ):
            continue

        # Query column value first, when filtering
        if (
            hasattr(value, "__clause_element__")
            or isinstance(value, sql.ClauseElement)
        ):
            value = value.__clause_element__()

        d[column.name] = value

    return d


def exists(session, instance):

    session_autoflush = session.autoflush
    session.autoflush = False

    persistent = session.query(
        instance.__class__
    ).filter_by(
        **get_filter_by_kwargs(instance)
    ).first()

    session.autoflush = session_autoflush

    return persistent or instance


def filter_instance(session, instance):
    """
    session `before_attach` event listener

    recursively walk through relationships of instance
    and verify that the instance, and related instances
    aren't already have state.persistent before adding a new instance
    to the session in state.pending
    """

    pass

    # state = attributes.instance_state(instance)
    # mapper = state.mapper
    # relationships = mapper.relationships

    # for relationship in relationships:
    #     pass

    # for o, m, st_, dct_ in mapper.cascade_iterator(
    #     "save-update", state, halt_on=session._contains_state
    # )

    # exists = session.query(
    #     instance.__class__
    # ).filter_by(
    #     **get_filter_by_kwargs(instance)
    # ).first()

    # if exists is not None:
    #     instance = exists

    # filter_kwargs = {}

    # columns = instance.__table__.columns
    # for column in columns:
    #     if column.primary

    #     state._orphaned_outside_of_session = False
    #     self._save_or_update_impl(state)

    #     mapper = _state_mapper(state)
    #     for o, m, st_, dct_ in mapper.cascade_iterator(
    #         "save-update", state, halt_on=self._contains_state
    #     ):
    #         self._save_or_update_impl(st_)
