"""created db structure

Revision ID: 55d2bf274509
Revises: 1f3303fa65d2
Create Date: 2021-08-03 10:13:37.247668

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '55d2bf274509'
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    # CREATE TABLE "action" (
    #   "status" varchar PRIMARY KEY
    # );
    action_table = op.create_table('action', sa.Column('status', sa.String(), nullable=False, primary_key=True))
    op.bulk_insert(action_table, [{'status': 'initial'}, {'status': 'retry'}, {'status': 'delete'},])

    # CREATE TABLE "parser" (
    #   "name" varchar PRIMARY KEY
    #   "source_filename" varchar,
    # );
    action_table = op.create_table('parser',
                                   sa.Column('name', sa.String(), nullable=False, primary_key=True),
                                   sa.Column('source_pattern', sa.String(), nullable=False),
                                   sa.Column('reference_service_endpoint', sa.String(), nullable=False),
    )
    op.bulk_insert(action_table, [
        {'name': 'arXiv', 'source_pattern': r'^.\d{4,5}.raw$', 'reference_service_endpoint': '/text'},
        {'name': 'AGU', 'source_pattern': '^.agu.xml', 'reference_service_endpoint': '/xml'},
        {'name': 'AIP', 'source_pattern': '^.aip.xml', 'reference_service_endpoint': '/xml'},
        {'name': 'APS', 'source_pattern': '^.ref.xml', 'reference_service_endpoint': '/xml'},
        {'name': 'CrossRef', 'source_pattern': '^.xref.xml', 'reference_service_endpoint': '/xml'},
        {'name': 'ELSEVIER', 'source_pattern': '^.elsevier.xml', 'reference_service_endpoint': '/xml'},
        {'name': 'IOP', 'source_pattern': '^.iop.xml', 'reference_service_endpoint': '/xml'},
        {'name': 'JATS', 'source_pattern': '^.jats.xml', 'reference_service_endpoint': '/xml'},
        {'name': 'NATURE', 'source_pattern': '^.nature.xml', 'reference_service_endpoint': '/xml'},
        {'name': 'NLM', 'source_pattern': '^.nlm3.xml', 'reference_service_endpoint': '/xml'},
        {'name': 'SPRINGER', 'source_pattern': '^.springer.xml', 'reference_service_endpoint': '/xml'},
        {'name': 'WILEY', 'source_pattern': '^.wiley2.xml', 'reference_service_endpoint': '/xml'},
    ])

    # CREATE TABLE "reference_source" (
    #   "bibcode" varchar(20),
    #   "source_filename" varchar,
    #   "source_create" datetime,
    #   "resolved_filename" varchar,
    #   "parser_name" varchar,
    #   PRIMARY KEY ("bibcode", "source_filename")
    # );
    op.create_table('reference_source',
                    sa.Column('bibcode', sa.String(), nullable=False, primary_key=True),
                    sa.Column('source_filename', sa.String(), nullable=False, primary_key=True),
                    sa.Column('resolved_filename', sa.String(), nullable=False),
                    sa.Column('parser_name', sa.String(), nullable=False),
                    sa.ForeignKeyConstraint(('parser_name',), ['parser.name'], ),
    )

    # CREATE TABLE "processed_history" (
    #   "id" integer PRIMARY KEY,
    #   "bibcode" varchar(20),
    #   "source_filename" varchar,
    #   "source_modified" datetime,
    #   "status" varchar,
    #   "date" datetime,
    #   "total_ref" integer,
    #   "resolved_ref" integer
    #   Foreign KEY ("bibcode", "source_filename")
    # );
    op.create_table('processed_history',
                    sa.Column('id', sa.Integer(), nullable=False, primary_key=True, autoincrement=True),
                    sa.Column('bibcode', sa.String(), nullable=False),
                    sa.Column('source_filename', sa.String(), nullable=False),
                    sa.ForeignKeyConstraint(['bibcode', 'source_filename'], ['reference_source.bibcode','reference_source.source_filename'], ),
                    sa.Column('source_modified', sa.DateTime(timezone=True), nullable=False),
                    sa.Column('status', sa.String(), nullable=False),
                    sa.ForeignKeyConstraint(('status',), ['action.status'], ),
                    sa.Column('date', sa.DateTime(timezone=True), nullable=False),
                    sa.Column('total_ref', sa.Integer(), nullable=False),
    )

    # CREATE TABLE "resolved_reference" (
    #   "history_id" integer,
    #   "item_num" integer,
    #   "reference_str" text,
    #   "bibcode" text,
    #   "score" numeric,
    #   "reference_raw" text,
    #   PRIMARY KEY ("history_id", "item_num")
    # );
    op.create_table('resolved_reference',
                    sa.Column('history_id', sa.Integer(), nullable=False, primary_key=True),
                    sa.Column('item_num', sa.Integer(), nullable=False, primary_key=True),
                    sa.Column('reference_str', sa.String(), nullable=False),
                    sa.Column('bibcode', sa.String(), nullable=False),
                    sa.Column('score', sa.Numeric(), nullable=False),
                    sa.Column('reference_raw', sa.String(), nullable=False),
    )

    # CREATE TABLE "compare" (
    #   "history_id" integer,
    #   "item_num" integer,
    #   "bibcode" text,
    #   "score" numeric,
    #   "state" text,
    #   Foreign KEY ("history_id", "item_num")
    # );
    op.create_table('compare',
                    sa.Column('history_id', sa.Integer(), nullable=False),
                    sa.Column('item_num', sa.Integer(), nullable=False),
                    sa.ForeignKeyConstraint(['history_id', 'item_num'], ['resolved_reference.history_id','resolved_reference.item_num'], ),
                    sa.Column('bibcode', sa.String(), nullable=False),
                    sa.Column('score', sa.Numeric(), nullable=False),
                    sa.Column('state', sa.String(), nullable=False),
    )

    # temparary table to tell what is the class of arxiv bibcode for verification proposes only
    # created this table directly in postgres
    # CREATE TABLE "arxiv" (
    #   "bibcode" text,
    #   "category" text,
    #   PRIMARY KEY ("bibcode", "category")
    # );



def downgrade():
    op.drop_table('compare')
    op.drop_table('resolved')
    op.drop_table('history')
    op.drop_table('reference')
    op.drop_table('parser')
    op.drop_table('action')
