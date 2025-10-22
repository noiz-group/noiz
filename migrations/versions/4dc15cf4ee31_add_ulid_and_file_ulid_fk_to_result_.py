"""add_ulid_and_file_ulid_fk_to_result_entities

Revision ID: 4dc15cf4ee31
Revises: 3c828f433797
Create Date: 2025-10-16 18:09:03.444481

"""
from alembic import op
import sqlalchemy as sa
import noiz


# revision identifiers, used by Alembic.
revision = '4dc15cf4ee31'
down_revision = '3c828f433797'
branch_labels = None
depends_on = None


def upgrade():
    from ulid import ULID
    from sqlalchemy import text

    connection = op.get_bind()

    # T063: Add ULID columns to result entities
    op.add_column('datachunk', sa.Column('ulid', sa.String(length=26), nullable=True))
    op.add_column('crosscorrelation_cartesian', sa.Column('ulid', sa.String(length=26), nullable=True))
    op.add_column('crosscorrelation_cylindrical', sa.Column('ulid', sa.String(length=26), nullable=True))
    op.add_column('processeddatachunk', sa.Column('ulid', sa.String(length=26), nullable=True))
    op.add_column('beamforming_result', sa.Column('ulid', sa.String(length=26), nullable=True))
    op.add_column('ppsd_result', sa.Column('ulid', sa.String(length=26), nullable=True))
    op.add_column('ccfstack', sa.Column('ulid', sa.String(length=26), nullable=True))

    # T064: Add file_ulid FK columns (nullable initially)
    op.add_column('datachunk', sa.Column('file_ulid', sa.String(length=26), nullable=True))
    op.add_column('crosscorrelation_cartesian', sa.Column('file_ulid', sa.String(length=26), nullable=True))
    op.add_column('crosscorrelation_cylindrical', sa.Column('file_ulid', sa.String(length=26), nullable=True))
    op.add_column('processeddatachunk', sa.Column('file_ulid', sa.String(length=26), nullable=True))
    op.add_column('beamforming_result', sa.Column('file_ulid', sa.String(length=26), nullable=True))
    op.add_column('ppsd_result', sa.Column('file_ulid', sa.String(length=26), nullable=True))

    # Backfill ULIDs for result entities
    for table in ['datachunk', 'crosscorrelation_cartesian', 'crosscorrelation_cylindrical',
                  'processeddatachunk', 'beamforming_result', 'ppsd_result', 'ccfstack']:
        result = connection.execute(text(f"SELECT id FROM {table} WHERE ulid IS NULL"))
        for row in result:
            new_ulid = str(ULID())
            connection.execute(text(f"UPDATE {table} SET ulid = :ulid WHERE id = :id"),
                             {"ulid": new_ulid, "id": row.id})

    # T065: Backfill file_ulid from existing file_id relationships
    # datachunk: datachunk_file_id → datachunk_file.ulid
    connection.execute(text("""
        UPDATE datachunk dc
        SET file_ulid = df.ulid
        FROM datachunk_file df
        WHERE dc.datachunk_file_id = df.id AND dc.file_ulid IS NULL
    """))

    # crosscorrelation_cartesian
    connection.execute(text("""
        UPDATE crosscorrelation_cartesian cc
        SET file_ulid = cf.ulid
        FROM crosscorrelation_cartesian_file cf
        WHERE cc.crosscorrelation_cartesian_file_id = cf.id AND cc.file_ulid IS NULL
    """))

    # crosscorrelation_cylindrical
    connection.execute(text("""
        UPDATE crosscorrelation_cylindrical cc
        SET file_ulid = cf.ulid
        FROM crosscorrelation_cylindrical_file cf
        WHERE cc.crosscorrelation_cylindrical_file_id = cf.id AND cc.file_ulid IS NULL
    """))

    # processeddatachunk
    connection.execute(text("""
        UPDATE processeddatachunk pd
        SET file_ulid = pf.ulid
        FROM processed_datachunk_file pf
        WHERE pd.processed_datachunk_file_id = pf.id AND pd.file_ulid IS NULL
    """))

    # beamforming_result
    connection.execute(text("""
        UPDATE beamforming_result br
        SET file_ulid = bf.ulid
        FROM beamforming_file bf
        WHERE br.beamforming_file_id = bf.id AND br.file_ulid IS NULL
    """))

    # ppsd_result
    connection.execute(text("""
        UPDATE ppsd_result pr
        SET file_ulid = pf.ulid
        FROM ppsd_file pf
        WHERE pr.ppsd_file_id = pf.id AND pr.file_ulid IS NULL
    """))

    # Make ulid NOT NULL on all result entities
    op.alter_column('datachunk', 'ulid', nullable=False)
    op.alter_column('crosscorrelation_cartesian', 'ulid', nullable=False)
    op.alter_column('crosscorrelation_cylindrical', 'ulid', nullable=False)
    op.alter_column('processeddatachunk', 'ulid', nullable=False)
    op.alter_column('beamforming_result', 'ulid', nullable=False)
    op.alter_column('ppsd_result', 'ulid', nullable=False)
    op.alter_column('ccfstack', 'ulid', nullable=False)

    # Add unique constraints on ulid
    op.create_unique_constraint('uq_datachunk_ulid', 'datachunk', ['ulid'])
    op.create_unique_constraint('uq_crosscorrelation_cartesian_ulid', 'crosscorrelation_cartesian', ['ulid'])
    op.create_unique_constraint('uq_crosscorrelation_cylindrical_ulid', 'crosscorrelation_cylindrical', ['ulid'])
    op.create_unique_constraint('uq_processeddatachunk_ulid', 'processeddatachunk', ['ulid'])
    op.create_unique_constraint('uq_beamforming_result_ulid', 'beamforming_result', ['ulid'])
    op.create_unique_constraint('uq_ppsd_result_ulid', 'ppsd_result', ['ulid'])
    op.create_unique_constraint('uq_ccfstack_ulid', 'ccfstack', ['ulid'])

    # Create indexes on ulid
    op.create_index('idx_datachunk_ulid', 'datachunk', ['ulid'])
    op.create_index('idx_crosscorrelation_cartesian_ulid', 'crosscorrelation_cartesian', ['ulid'])
    op.create_index('idx_crosscorrelation_cylindrical_ulid', 'crosscorrelation_cylindrical', ['ulid'])
    op.create_index('idx_processeddatachunk_ulid', 'processeddatachunk', ['ulid'])
    op.create_index('idx_beamforming_result_ulid', 'beamforming_result', ['ulid'])
    op.create_index('idx_ppsd_result_ulid', 'ppsd_result', ['ulid'])
    op.create_index('idx_ccfstack_ulid', 'ccfstack', ['ulid'])

    # T066: Add foreign key constraints on file_ulid columns
    op.create_foreign_key('fk_datachunk_file_ulid', 'datachunk', 'datachunk_file',
                          ['file_ulid'], ['ulid'])
    op.create_foreign_key('fk_crosscorrelation_cartesian_file_ulid', 'crosscorrelation_cartesian',
                          'crosscorrelation_cartesian_file', ['file_ulid'], ['ulid'])
    op.create_foreign_key('fk_crosscorrelation_cylindrical_file_ulid', 'crosscorrelation_cylindrical',
                          'crosscorrelation_cylindrical_file', ['file_ulid'], ['ulid'])
    op.create_foreign_key('fk_processeddatachunk_file_ulid', 'processeddatachunk',
                          'processed_datachunk_file', ['file_ulid'], ['ulid'])
    op.create_foreign_key('fk_beamforming_result_file_ulid', 'beamforming_result',
                          'beamforming_file', ['file_ulid'], ['ulid'])
    op.create_foreign_key('fk_ppsd_result_file_ulid', 'ppsd_result',
                          'ppsd_file', ['file_ulid'], ['ulid'])


def downgrade():
    # Drop FK constraints
    op.drop_constraint('fk_ppsd_result_file_ulid', 'ppsd_result', type_='foreignkey')
    op.drop_constraint('fk_beamforming_result_file_ulid', 'beamforming_result', type_='foreignkey')
    op.drop_constraint('fk_processeddatachunk_file_ulid', 'processeddatachunk', type_='foreignkey')
    op.drop_constraint('fk_crosscorrelation_cylindrical_file_ulid', 'crosscorrelation_cylindrical', type_='foreignkey')
    op.drop_constraint('fk_crosscorrelation_cartesian_file_ulid', 'crosscorrelation_cartesian', type_='foreignkey')
    op.drop_constraint('fk_datachunk_file_ulid', 'datachunk', type_='foreignkey')

    # Drop indexes
    op.drop_index('idx_ccfstack_ulid', table_name='ccfstack')
    op.drop_index('idx_ppsd_result_ulid', table_name='ppsd_result')
    op.drop_index('idx_beamforming_result_ulid', table_name='beamforming_result')
    op.drop_index('idx_processeddatachunk_ulid', table_name='processeddatachunk')
    op.drop_index('idx_crosscorrelation_cylindrical_ulid', table_name='crosscorrelation_cylindrical')
    op.drop_index('idx_crosscorrelation_cartesian_ulid', table_name='crosscorrelation_cartesian')
    op.drop_index('idx_datachunk_ulid', table_name='datachunk')

    # Drop unique constraints
    op.drop_constraint('uq_ccfstack_ulid', 'ccfstack', type_='unique')
    op.drop_constraint('uq_ppsd_result_ulid', 'ppsd_result', type_='unique')
    op.drop_constraint('uq_beamforming_result_ulid', 'beamforming_result', type_='unique')
    op.drop_constraint('uq_processeddatachunk_ulid', 'processeddatachunk', type_='unique')
    op.drop_constraint('uq_crosscorrelation_cylindrical_ulid', 'crosscorrelation_cylindrical', type_='unique')
    op.drop_constraint('uq_crosscorrelation_cartesian_ulid', 'crosscorrelation_cartesian', type_='unique')
    op.drop_constraint('uq_datachunk_ulid', 'datachunk', type_='unique')

    # Drop file_ulid columns
    op.drop_column('ppsd_result', 'file_ulid')
    op.drop_column('beamforming_result', 'file_ulid')
    op.drop_column('processeddatachunk', 'file_ulid')
    op.drop_column('crosscorrelation_cylindrical', 'file_ulid')
    op.drop_column('crosscorrelation_cartesian', 'file_ulid')
    op.drop_column('datachunk', 'file_ulid')

    # Drop ulid columns
    op.drop_column('ccfstack', 'ulid')
    op.drop_column('ppsd_result', 'ulid')
    op.drop_column('beamforming_result', 'ulid')
    op.drop_column('processeddatachunk', 'ulid')
    op.drop_column('crosscorrelation_cylindrical', 'ulid')
    op.drop_column('crosscorrelation_cartesian', 'ulid')
    op.drop_column('datachunk', 'ulid')
