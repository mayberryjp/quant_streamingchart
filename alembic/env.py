from __future__ import annotations

from logging.config import fileConfig

from alembic import context
from sqlalchemy import engine_from_config, pool

from streamchart.config import settings
from streamchart.models import metadata

config = context.config

if config.config_file_name is not None:
    fileConfig(config.config_file_name)

config.set_main_option("sqlalchemy.url", settings.database_url)

target_metadata = metadata
VERSION_TABLE = "alembic_version_streamchart"


def include_name(name: str | None, type_: str, parent_names: dict[str, str | None]) -> bool:
    # On a shared database, only manage this service's own tables so autogenerate
    # never proposes dropping tables owned by other applications.
    if type_ == "table":
        return name in target_metadata.tables
    return True


def run_migrations_offline() -> None:
    context.configure(
        url=settings.database_url,
        target_metadata=target_metadata,
        literal_binds=True,
        version_table=VERSION_TABLE,
        include_name=include_name,
        include_schemas=False,
        dialect_opts={"paramstyle": "named"},
    )
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    section = config.get_section(config.config_ini_section, {})
    connectable = engine_from_config(section, prefix="sqlalchemy.", poolclass=pool.NullPool)
    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            version_table=VERSION_TABLE,
            include_name=include_name,
            include_schemas=False,
        )
        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
