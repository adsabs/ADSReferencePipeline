"""add_external_identifier

Revision ID: 08ca70bd6f5f
Revises: e3d6e15c3b8c
Create Date: 2026-01-05 11:16:27.454389

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


# revision identifiers, used by Alembic.
revision = '08ca70bd6f5f'
down_revision = 'e3d6e15c3b8c'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column('resolved_reference',
                    sa.Column("external_identifier",
                    postgresql.ARRAY(sa.String()))
                    )


def downgrade():
    op.drop_column('resolved_reference', 'external_identifier')

