"""
Script to anonymize a CSV dump of parkeerrechten so that it may be used in
tests and committed with the code.
"""
# Note: that the pandas library is needed and not installed for this project
# by default. This script is merely included for reference, and is only useful
# if you have a CSV dump of parkeerrechten.
import sys
import random
import uuid
from hashlib import md5
from collections import OrderedDict

import pandas as pd

column_defs = OrderedDict([
    ('VERW_RECHT_ID', int),
    ('LAND_C_V_RECHT', str),
    ('VERK_P_V_RECHT', int),
    ('VERK_PUNT_OMS', str),

    ('B_TYD_V_RECHT', str),
    ('E_TYD_V_RECHT', str),
    ('E_TYD_R_AANP', str),
    ('BEDRAG_V_RECHT', str),

    ('BTW_V_RECHT', str),
    ('BEDR_V_RECHT_B', str),
    ('BTW_V_RECHT_BER', str),
    ('BEDR_V_RECHT_H', str),

    ('BTW_V_RECHT_HER', str),
    ('TYD_HERBEREK', str),
    ('RECHTV_V_RECHT', str),
    ('RECHTV_INT_OMS', str),

    ('GEB_BEH_V_RECHT', int),
    ('GEBIEDS_BEH_OMS', str),
    ('GEB_C_V_RECHT', str),
    ('GEBIED_OMS', str),

    ('REG_TYD_V_RECHT', str),
    ('COORD_V_RECHT', str),
    ('GEBR_DOEL_RECHT', str),
    ('GEBR_DOEL_OMS', str),

    ('R_TYD_E_TYD_VR', str),
    ('VER_BATCH_ID', int),
    ('VER_BATCH_NAAM', str),
    ('KENM_RECHTV_INT', str)
])

COLUMNS_TO_PERMUTE = [
    'LAND_C_V_RECHT',
    'VERK_P_V_RECHT',
    'VERK_PUNT_OMS',
    'BEDRAG_V_RECHT',
    'BTW_V_RECHT',
    'BEDR_V_RECHT_B',
    'BTW_V_RECHT_BER',
    'BEDR_V_RECHT_H',
    'BTW_V_RECHT_HER',
    'GEB_C_V_RECHT',
    'GEBIED_OMS',
    'COORD_V_RECHT',
    'GEBR_DOEL_RECHT',
    'GEBR_DOEL_OMS',
    'RECHTV_V_RECHT',
    'RECHTV_INT_OMS',
]

COLUMNS_TO_RANDOMIZE_TIME = [
    'B_TYD_V_RECHT',
    'E_TYD_V_RECHT',
    'E_TYD_R_AANP',
    'TYD_HERBEREK',
    'REG_TYD_V_RECHT',
    'R_TYD_E_TYD_VR',
]


def randomize_time(s):
    """Assumes datetime in YYYYMMDDHHMMSS format, randomizes HHMMSS part."""
    return s[:-6] + '%02d%o2d%02d' % (
        random.randint(0, 23), random.randint(0, 59), random.randint(0, 59))


def anonymize(filename, nrows=10):
    """Anonymize CSV dump of parkeerrechten data for tests."""
    df = pd.read_csv(filename, names=column_defs.keys(), dtype=column_defs,
                     header=0, nrows=nrows)
    nan = float('nan')

    for idx, row in df.iterrows():
        # destroy the unique ids:
        df.at[idx, 'VERW_RECHT_ID'] = random.randint(9_000_000_000, 9_999_999_999) # noqa

        # destroy the column that is somtimes identifiable:
        df.at[idx, 'KENM_RECHTV_INT'] = \
            md5(str(uuid.uuid4()).encode('ascii')).hexdigest()

        # Randomize the time of day; leave date intact (to match batch name).
        # Note: without regard for the link between begin and end times.
        for c in COLUMNS_TO_RANDOMIZE_TIME:
            if not pd.isnull(row[c]):
                df.at[idx, c] = randomize_time(row[c])

    # permute the values in data frame columns (indepently)
    for c in COLUMNS_TO_PERMUTE:
        df[c] = df[c].sample(frac=1).values

    return df


if __name__ == '__main__':
    print('Usage: python anonymize <input.csv> <output.csv> <number of rows>')
    assert len(sys.argv) == 4
    df = anonymize(sys.argv[1], int(sys.argv[3]))
    df.to_csv(sys.argv[2], index=False)
    print('Done anonymizing %d records' % len(df))
