"""add_ulid_fields_to_file_entities

Revision ID: 3c828f433797
Revises: 8c9b2ea10904
Create Date: 2025-10-16 17:58:10.842800

"""
from alembic import op
import sqlalchemy as sa
import noiz


# revision identifiers, used by Alembic.
revision = '3c828f433797'
down_revision = '8c9b2ea10904'
branch_labels = None
depends_on = None


def upgrade():
    # T057: Add ULID columns to file entities (nullable=True initially for backfill)
    op.add_column('datachunk_file', sa.Column('ulid', sa.String(length=26), nullable=True))
    op.add_column('crosscorrelation_cartesian_file', sa.Column('ulid', sa.String(length=26), nullable=True))
    op.add_column('crosscorrelation_cylindrical_file', sa.Column('ulid', sa.String(length=26), nullable=True))
    op.add_column('processed_datachunk_file', sa.Column('ulid', sa.String(length=26), nullable=True))
    op.add_column('beamforming_file', sa.Column('ulid', sa.String(length=26), nullable=True))
    op.add_column('ppsd_file', sa.Column('ulid', sa.String(length=26), nullable=True))

    # T058: Backfill ULIDs for existing records using Python's ULID generation
    from ulid import ULID
    from sqlalchemy import text

    connection = op.get_bind()

    # Backfill each table
    for table in ['datachunk_file', 'crosscorrelation_cartesian_file',
                  'crosscorrelation_cylindrical_file', 'processed_datachunk_file',
                  'beamforming_file', 'ppsd_file']:
        # Get all records without ULID
        result = connection.execute(text(f"SELECT id FROM {table} WHERE ulid IS NULL"))
        for row in result:
            new_ulid = str(ULID())
            connection.execute(text(f"UPDATE {table} SET ulid = :ulid WHERE id = :id"),
                             {"ulid": new_ulid, "id": row.id})

    # T059: Make ULID columns NOT NULL
    op.alter_column('datachunk_file', 'ulid', nullable=False)
    op.alter_column('crosscorrelation_cartesian_file', 'ulid', nullable=False)
    op.alter_column('crosscorrelation_cylindrical_file', 'ulid', nullable=False)
    op.alter_column('processed_datachunk_file', 'ulid', nullable=False)
    op.alter_column('beamforming_file', 'ulid', nullable=False)
    op.alter_column('ppsd_file', 'ulid', nullable=False)

    # T060: Add unique constraints
    op.create_unique_constraint('uq_datachunk_file_ulid', 'datachunk_file', ['ulid'])
    op.create_unique_constraint('uq_crosscorrelation_cartesian_file_ulid', 'crosscorrelation_cartesian_file', ['ulid'])
    op.create_unique_constraint('uq_crosscorrelation_cylindrical_file_ulid', 'crosscorrelation_cylindrical_file', ['ulid'])
    op.create_unique_constraint('uq_processed_datachunk_file_ulid', 'processed_datachunk_file', ['ulid'])
    op.create_unique_constraint('uq_beamforming_file_ulid', 'beamforming_file', ['ulid'])
    op.create_unique_constraint('uq_ppsd_file_ulid', 'ppsd_file', ['ulid'])

    # T061: Create indexes for fast lookups
    op.create_index('idx_datachunk_file_ulid', 'datachunk_file', ['ulid'])
    op.create_index('idx_crosscorrelation_cartesian_file_ulid', 'crosscorrelation_cartesian_file', ['ulid'])
    op.create_index('idx_crosscorrelation_cylindrical_file_ulid', 'crosscorrelation_cylindrical_file', ['ulid'])
    op.create_index('idx_processed_datachunk_file_ulid', 'processed_datachunk_file', ['ulid'])
    op.create_index('idx_beamforming_file_ulid', 'beamforming_file', ['ulid'])
    op.create_index('idx_ppsd_file_ulid', 'ppsd_file', ['ulid'])


def downgrade():
    # Drop indexes
    op.drop_index('idx_ppsd_file_ulid', table_name='ppsd_file')
    op.drop_index('idx_beamforming_file_ulid', table_name='beamforming_file')
    op.drop_index('idx_processed_datachunk_file_ulid', table_name='processed_datachunk_file')
    op.drop_index('idx_crosscorrelation_cylindrical_file_ulid', table_name='crosscorrelation_cylindrical_file')
    op.drop_index('idx_crosscorrelation_cartesian_file_ulid', table_name='crosscorrelation_cartesian_file')
    op.drop_index('idx_datachunk_file_ulid', table_name='datachunk_file')

    # Drop unique constraints
    op.drop_constraint('uq_ppsd_file_ulid', 'ppsd_file', type_='unique')
    op.drop_constraint('uq_beamforming_file_ulid', 'beamforming_file', type_='unique')
    op.drop_constraint('uq_processed_datachunk_file_ulid', 'processed_datachunk_file', type_='unique')
    op.drop_constraint('uq_crosscorrelation_cylindrical_file_ulid', 'crosscorrelation_cylindrical_file', type_='unique')
    op.drop_constraint('uq_crosscorrelation_cartesian_file_ulid', 'crosscorrelation_cartesian_file', type_='unique')
    op.drop_constraint('uq_datachunk_file_ulid', 'datachunk_file', type_='unique')

    # Drop ULID columns
    op.drop_column('ppsd_file', 'ulid')
    op.drop_column('beamforming_file', 'ulid')
    op.drop_column('processed_datachunk_file', 'ulid')
    op.drop_column('crosscorrelation_cylindrical_file', 'ulid')
    op.drop_column('crosscorrelation_cartesian_file', 'ulid')
    op.drop_column('datachunk_file', 'ulid')
