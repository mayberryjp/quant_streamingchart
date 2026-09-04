from __future__ import annotations

from logging.config import fileConfig

from alembic import context
from sqlalchemy import Connection, engine_from_config, pool, text

from streamchart.config import settings
from streamchart.models import SCHEMA_NAME, metadata

config = context.config

if config.config_file_name is not None:
    fileConfig(config.config_file_name)

config.set_main_option("sqlalchemy.url", settings.database_url)

target_metadata = metadata
VERSION_TABLE = "alembic_version_streamchart"


def include_name(name: str | None, type_: str, parent_names: dict[str, str | None]) -> bool:
    # On a shared database, only manage this service's own tables so autogenerate
    # never proposes dropping tables owned by other applications.
    if type_ == "schema":
        return name == SCHEMA_NAME
    if type_ == "table":
        schema = parent_names.get("schema_name")
        key = f"{schema}.{name}" if schema else name
        return key in target_metadata.tables
    return True


def _relocate_existing_tables(connection: Connection) -> None:
    """Ensure the target schema exists and pull in any of this service's tables
    (including its Alembic version table) that a prior deployment created in a
    different schema, e.g. the default ``public`` schema."""
    connection.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{SCHEMA_NAME}"'))
    table_names = [table.name for table in target_metadata.tables.values()]
    table_names.append(VERSION_TABLE)
    for name in table_names:
        connection.execute(
            text(
                f"""
                DO $$
                DECLARE src text;
                BEGIN
                    SELECT n.nspname INTO src
                    FROM pg_class c
                    JOIN pg_namespace n ON n.oid = c.relnamespace
                    WHERE c.relkind = 'r'
                      AND c.relname = '{name}'
                      AND n.nspname <> '{SCHEMA_NAME}'
                    LIMIT 1;
                    IF src IS NOT NULL AND NOT EXISTS (
                        SELECT 1 FROM pg_class c
                        JOIN pg_namespace n ON n.oid = c.relnamespace
                        WHERE c.relname = '{name}' AND n.nspname = '{SCHEMA_NAME}'
                    ) THEN
                        EXECUTE format(
                            'ALTER TABLE %I.%I SET SCHEMA %I', src, '{name}', '{SCHEMA_NAME}'
                        );
                    END IF;
                END $$;
                """
            )
        )


def run_migrations_offline() -> None:
    context.configure(
        url=settings.database_url,
        target_metadata=target_metadata,
        literal_binds=True,
        version_table=VERSION_TABLE,
        version_table_schema=SCHEMA_NAME,
        include_name=include_name,
        include_schemas=True,
        dialect_opts={"paramstyle": "named"},
    )
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    section = config.get_section(config.config_ini_section, {})
    connectable = engine_from_config(section, prefix="sqlalchemy.", poolclass=pool.NullPool)
    with connectable.connect() as connection:
        _relocate_existing_tables(connection)
        # New tables created by unqualified migration ops land in the target schema.
        connection.execute(text(f'SET search_path TO "{SCHEMA_NAME}", public'))
        connection.commit()
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            version_table=VERSION_TABLE,
            version_table_schema=SCHEMA_NAME,
            include_name=include_name,
            include_schemas=True,
        )
        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
