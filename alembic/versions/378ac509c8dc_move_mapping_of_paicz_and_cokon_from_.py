"""Move mapping of PAICz and CoKon from ADStex to ADStxt

Revision ID: 378ac509c8dc
Revises: 55d2bf274509
Create Date: 2024-10-11 10:44:50.306251

"""
from alembic import op
import sqlalchemy as sa

from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = '378ac509c8dc'
down_revision = '55d2bf274509'
branch_labels = None
depends_on = None

def move_mapping(journal, current_parser, future_parser):
    # Move the journal from current_parser to future_parser
    op.execute(f"""
    UPDATE parser
    SET matches = (
        SELECT jsonb_agg(elem)
        FROM jsonb_array_elements(matches) elem
        WHERE elem != '{{"journal": "{journal}", "all_volume": true}}'
    )
    WHERE name = '{current_parser}';
    """)

    op.execute(f"""
    UPDATE parser
    SET matches = matches || '{{"journal": "{journal}", "all_volume": true}}'
    WHERE name = '{future_parser}';
    """)

def upgrade():
    move_mapping('PAICz', 'ADStex', 'ADStxt')
    move_mapping('CoKon', 'ADStex', 'ADStxt')


def downgrade():
    move_mapping('PAICz', 'ADStxt', 'ADStex')
    move_mapping('CoKon', 'ADStxt', 'ADStex')
