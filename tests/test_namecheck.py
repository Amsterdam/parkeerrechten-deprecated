# noqa
import pytest

from parkeerrechten import namecheck
from parkeerrechten import settings


TEST_BATCH_NAME = '20170606'
TEST_BATCH_FILE = '20170606_' + settings.BACKUP_FILE_BASENAME + '.dump'


def test_parse_date_string():
    y, m, d = namecheck.parse_date_string('20170623')
    assert y == 2017
    assert m == 6
    assert d == 23


def test_is_batch_name():
    assert namecheck.is_batch_name('Leeg', True)
    assert not namecheck.is_batch_name('GARBAGE', False)

    assert namecheck.is_batch_name('20170504', False)


def test_filter_batch_names():
    filtered = namecheck.filter_batch_names(
        ['NOPE', 'Leeg', 'GARBAGE', TEST_BATCH_FILE, TEST_BATCH_NAME], True)

    assert filtered == ['Leeg', TEST_BATCH_NAME]


def test_filter_batch_names_by_date():
    test_dates = ['20170101', '20170201', '20170301', '20170401']
    func = namecheck.filter_batch_names_by_date

    assert len(func(test_dates, None, None)) == 4
    assert len(func(test_dates, '20170801', None)) == 0
    assert len(func(test_dates, None, '20161231')) == 0


def test_is_batch_file():
    assert namecheck.is_batch_file(TEST_BATCH_FILE, True)


def test_extract_batch_name():
    batch_name = namecheck.extract_batch_name(TEST_BATCH_FILE)
    assert batch_name == TEST_BATCH_NAME
