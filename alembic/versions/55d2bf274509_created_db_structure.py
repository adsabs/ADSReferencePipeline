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
                                   sa.Column('source_extension', sa.String(), nullable=False),
    )
    op.bulk_insert(action_table, [{'name': 'AGU', 'source_extension': '.agu.xml'},
                                  {'name': 'AIP', 'source_extension': '.aip.xml'},
                                  {'name': 'APS', 'source_extension': '.ref.xml'},
                                  {'name': 'CrossRef', 'source_extension': '.xref.xml'},
                                  {'name': 'ELSEVIER', 'source_extension': '.elsevier.xml'},
                                  {'name': 'IOP', 'source_extension': '.iop.xml'},
                                  {'name': 'JATS', 'source_extension': '.jats.xml'},
                                  {'name': 'NATURE', 'source_extension': '.nature.xml'},
                                  {'name': 'NLM', 'source_extension': '.nlm3.xml'},
                                  {'name': 'SPRINGER', 'source_extension': '.springer.xml'},
                                  {'name': 'Text', 'source_extension': '.raw'},
                                  {'name': 'WILEY', 'source_extension': '.wiley2.xml'},
                                 ])

    # CREATE TABLE "reference" (
    #   "bibcode" varchar(20),
    #   "source_filename" varchar,
    #   "source_create" datetime,
    #   "resolved_filename" varchar,
    #   PRIMARY KEY ("bibcode", "source_filename")
    # );
    op.create_table('reference',
                    sa.Column('bibcode', sa.String(), nullable=False, primary_key=True),
                    sa.Column('source_filename', sa.String(), nullable=False, primary_key=True),
                    sa.Column('resolved_filename', sa.String(), nullable=False),
                    sa.Column('parser', sa.String(), nullable=False),
                    sa.ForeignKeyConstraint(('parser',), ['parser.name'], ),
    )

    # CREATE TABLE "history" (
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
    op.create_table('history',
                    sa.Column('id', sa.Integer(), nullable=False, primary_key=True, autoincrement=True),
                    sa.Column('bibcode', sa.String(), nullable=False),
                    sa.Column('source_filename', sa.String(), nullable=False),
                    sa.ForeignKeyConstraint(['bibcode', 'source_filename'], ['reference.bibcode','reference.source_filename'], ),
                    sa.Column('source_modified', sa.DateTime(), nullable=False),
                    sa.Column('status', sa.String(), nullable=False),
                    sa.ForeignKeyConstraint(('status',), ['action.status'], ),
                    sa.Column('date', sa.DateTime(), nullable=False),
                    sa.Column('total_ref', sa.Integer(), nullable=False),
    )

    # CREATE TABLE "resolved" (
    #   "history_id" integer,
    #   "item_num" integer,
    #   "reference_str" text,
    #   "bibcode" text,
    #   "score" numeric,
    #   PRIMARY KEY ("history_id", "item_num")
    # );
    op.create_table('resolved',
                    sa.Column('history_id', sa.Integer(), nullable=False, primary_key=True),
                    sa.Column('item_num', sa.Integer(), nullable=False, primary_key=True),
                    sa.Column('reference_str', sa.String(), nullable=False),
                    sa.Column('bibcode', sa.String(), nullable=False),
                    sa.Column('score', sa.Numeric(), nullable=False),
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
                    sa.ForeignKeyConstraint(['history_id', 'item_num'], ['resolved.history_id','resolved.item_num'], ),
                    sa.Column('bibcode', sa.String(), nullable=False),
                    sa.Column('score', sa.Numeric(), nullable=False),
                    sa.Column('state', sa.String(), nullable=False),
    )

    # CREATE TABLE "xml" (
    #   "history_id" integer,
    #   "item_num" integer,
    #   "reference" text,
    #   Foreign KEY ("history_id", "item_num")
    # );
    op.create_table('xml',
                    sa.Column('history_id', sa.Integer(), nullable=False),
                    sa.Column('item_num', sa.Integer(), nullable=False),
                    sa.ForeignKeyConstraint(['history_id', 'item_num'], ['resolved.history_id','resolved.item_num'], ),
                    sa.Column('reference', sa.String(), nullable=False),
    )

    # temparary table to tell what is the class of arxiv bibcode for verification proposes only
    # created this table directly in postgres
    # CREATE TABLE "arxiv" (
    #   "bibcode" text,
    #   "category" text,
    #   PRIMARY KEY ("bibcode", "category")
    # );



def downgrade():
    op.drop_table('xml')
    op.drop_table('compare')
    op.drop_table('resolved')
    op.drop_table('history')
    op.drop_table('reference')
    op.drop_table('parser')
    op.drop_table('action')
