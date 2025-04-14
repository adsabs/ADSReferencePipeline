"""adding author to config

Revision ID: e3d6e15c3b8c
Revises: 378ac509c8dc
Create Date: 2025-04-14 07:36:41.122347

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'e3d6e15c3b8c'
down_revision = '378ac509c8dc'
branch_labels = None
depends_on = None


def upgrade():
    # Insert the new row for 'PairsTXTE6' for author journal
    op.execute("""
    INSERT INTO parser (name, extension_pattern, reference_service_endpoint, matches)
    VALUES ('PairsTXTE6', '.pairs', '/text', '[{"journal": "AUTHOR", "all_volume": true}]');
    """)


def downgrade():
    op.execute("""
    DELETE FROM parser WHERE name = 'PairsTXTE6';
    """)
