"""first revision

Revision ID: 280f4ed01fff
Revises:
Create Date: 2024-04-22 14:25:48.680435

"""

from typing import Sequence, Union

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "280f4ed01fff"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute("select 'It is the first revision to initialize Alembic'")
