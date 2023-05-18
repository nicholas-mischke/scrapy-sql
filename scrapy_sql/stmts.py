
from sqlalchemy import insert

# SQLAlchemy connection string format
# dialect+driver://username:password@host:port/database
def get_database_type(session):
    return session.bind.dialect.name


def insert(table, session):
    """
    Provides a SQLAlchemy INSERT statement that is compatible with the
    SQLite, MySQL, and PostgreSQL dialects.
    """
    return insert(table)


def insert_ignore(table, session):
    """
    Provides a SQLAlchemy INSERT IGNORE statement that is compatible with the
    SQLite, MySQL, and PostgreSQL dialects.
    """
    database_type = get_database_type(session)

    if database_type == 'sqlite':
        return insert(table).prefix_with('OR IGNORE')
    elif database_type == 'mysql':
        return insert(table).prefix_with('IGNORE')
    elif database_type == 'postgresql':
        return insert(table).on_conflict_do_nothing()


def upsert(table, session):
    """
    Provides a SQLAlchemy UPSERT statement that is compatible with the
    SQLite, MySQL, and PostgreSQL dialects.
    """
    database_type = get_database_type(session)

    if database_type == 'sqlite':
        return insert(table).prefix_with('OR REPLACE')
    elif database_type == 'mysql':
        return insert(table).prefix_with('ON DUPLICATE KEY UPDATE')
    elif database_type == 'postgresql':
        return insert(table).on_conflict_do_update()
