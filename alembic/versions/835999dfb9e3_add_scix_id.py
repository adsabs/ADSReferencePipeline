"""add scix_id

Revision ID: 835999dfb9e3
Revises: 08ca70bd6f5f
Create Date: 2026-02-11 12:45:45.441650

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '835999dfb9e3'
down_revision = '08ca70bd6f5f'
branch_labels = None
depends_on = None


def upgrade():
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    if "resolved_reference" not in inspector.get_table_names():
        raise RuntimeError(
            "Migration 835999dfb9e3 requires table `resolved_reference`, "
            "but it does not exist. Database schema and alembic_version are out of sync."
        )
    columns = {c["name"] for c in inspector.get_columns("resolved_reference")}
    if "scix_id" not in columns:
        op.add_column("resolved_reference", sa.Column("scix_id", sa.String(), nullable=True))


def downgrade():
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    if "resolved_reference" not in inspector.get_table_names():
        return
    columns = {c["name"] for c in inspector.get_columns("resolved_reference")}
    if "scix_id" in columns:
        op.drop_column("resolved_reference", "scix_id")
