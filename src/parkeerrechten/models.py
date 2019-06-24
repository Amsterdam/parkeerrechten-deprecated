"""
Queries for the Parkeerrechten database as hosted by Datapunt.
"""
# TODO: check whether we have to be explicit about encodings
from sqlalchemy import Table, Column, types


def get_backup_table_def(metadata, table_name):
    """
    Get SQLAlchemy core table definition to mirror that of NPR.

    Note: for local db during import and testing.
    """
    # the comments at the end of the lines are correcponding NPR datatypes

    table = Table(table_name, metadata,
        Column('VERW_RECHT_ID', types.Numeric(10, 0), nullable=False),  # noqa
        Column('LAND_C_V_RECHT', types.String(3)),        # varchar
        Column('VERK_P_V_RECHT', types.Numeric(10, 0)),   # numeric
        Column('VERK_PUNT_OMS', types.Unicode(80)),       # nvarchar
        Column('B_TYD_V_RECHT', types.String(14)),        # varchar
        Column('E_TYD_V_RECHT', types.String(14)),        # varchar
        Column('E_TYD_R_AANP', types.String(14)),         # varchar
        Column('BEDRAG_V_RECHT', types.Numeric(10, 2)),   # numeric
        Column('BTW_V_RECHT', types.Numeric(10, 2)),      # numeric
        Column('BEDR_V_RECHT_B', types.Numeric(10, 2)),   # numeric
        Column('BTW_V_RECHT_BER', types.Numeric(10, 2)),  # numeric
        Column('BEDR_V_RECHT_H', types.Numeric(10, 2)),   # numeric
        Column('BTW_V_RECHT_HER', types.Numeric(10, 2)),  # numeric
        Column('TYD_HERBEREK', types.String(14)),         # varchar
        Column('RECHTV_V_RECHT', types.String(10)),       # varchar
        Column('RECHTV_INT_OMS', types.Unicode(80)),      # nvarchar
        Column('GEB_BEH_V_RECHT', types.SmallInteger()),  # smallint 5
        Column('GEBIEDS_BEH_OMS', types.Unicode(80)),     # nvarchar
        Column('GEB_C_V_RECHT', types.String(10)),        # varchar
        Column('GEBIED_OMS', types.Unicode(80)),          # nvarchar
        Column('REG_TYD_V_RECHT', types.String(14)),      # varchar
        Column('COORD_V_RECHT', types.Unicode(30)),       # nvarchar
        Column('GEBR_DOEL_RECHT', types.String(10)),      # varchar
        Column('GEBR_DOEL_OMS', types.Unicode(80)),       # nvarchar
        Column('R_TYD_E_TYD_VR', types.String(14)),       # varchar
        Column('VER_BATCH_ID', types.Integer()),          # int 10
        Column('VER_BATCH_NAAM', types.String(12)),       # varchar
        Column('KENM_RECHTV_INT', types.Unicode(40))      # nvarchar
    )

    return table
