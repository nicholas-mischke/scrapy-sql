
def table_in_session(session, table):
    exists = session.query(
        table.__class__
    ).filter_by(
        **table.query_filter
    ).first()

    if exists is None:
        return False
    return True


def filter_table(session, table):
    return session.query(
        table.__class__
    ).filter_by(
        **table.query_filter
    ).first() or table
