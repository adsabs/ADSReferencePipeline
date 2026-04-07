"""add publication_year and refereed_status

Revision ID: 9a4b1e8b6c7d
Revises: 835999dfb9e3
Create Date: 2026-03-11 00:00:00.000000

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "9a4b1e8b6c7d"
down_revision = "835999dfb9e3"
branch_labels = None
depends_on = None


def upgrade():
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    if "resolved_reference" not in inspector.get_table_names():
        raise RuntimeError(
            "Migration 9a4b1e8b6c7d requires table `resolved_reference`, "
            "but it does not exist. Database schema and alembic_version are out of sync."
        )

    columns = {c["name"] for c in inspector.get_columns("resolved_reference")}
    if "publication_year" not in columns:
        op.add_column("resolved_reference", sa.Column("publication_year", sa.Integer(), nullable=True))
    if "refereed_status" not in columns:
        op.add_column("resolved_reference", sa.Column("refereed_status", sa.Integer(), nullable=True))


def downgrade():
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    if "resolved_reference" not in inspector.get_table_names():
        return

    columns = {c["name"] for c in inspector.get_columns("resolved_reference")}
    if "refereed_status" in columns:
        op.drop_column("resolved_reference", "refereed_status")
    if "publication_year" in columns:
        op.drop_column("resolved_reference", "publication_year")
