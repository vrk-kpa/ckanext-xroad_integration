"""Create error table

Revision ID: 7d4070e62886
Revises:
Create Date: 2023-01-17 07:46:03.674300

"""
import uuid

import six
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '7d4070e62886'
down_revision = None
branch_labels = None
depends_on = None


def make_uuid():
    return six.text_type(uuid.uuid4())


def upgrade():

    op.create_table('xroad_errors',
                    sa.Column('id', sa.types.UnicodeText, primary_key=True, default=make_uuid),
                    sa.Column('message', sa.types.UnicodeText, nullable=False),
                    sa.Column('code', sa.types.Integer, nullable=False),
                    sa.Column('created', sa.types.DateTime),
                    sa.Column('xroad_instance', sa.types.UnicodeText),
                    sa.Column('member_class', sa.types.UnicodeText),
                    sa.Column('member_code', sa.types.UnicodeText),
                    sa.Column('subsystem_code', sa.types.UnicodeText),
                    sa.Column('service_code', sa.types.UnicodeText),
                    sa.Column('service_version', sa.types.UnicodeText),
                    sa.Column('server_code', sa.types.UnicodeText),
                    sa.Column('security_category_code', sa.types.UnicodeText),
                    sa.Column('group_code', sa.types.UnicodeText))


def downgrade():
    op.drop_table('xroad_errors')
