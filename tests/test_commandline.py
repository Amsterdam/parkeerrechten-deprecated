# noqa
import pytest

from parkeerrechten import commandline


def test_no_options():
    args = commandline.parse_args([], include_orphans_option=True)
    assert args.enddate == None
    assert args.startdate == None
    assert args.orphans == False


def test_correct_dates():
    args = commandline.parse_args(
        [
            '--startdate', '20170101',
            '--enddate', '20170201'
        ],
        include_orphans_option=True
    )
    assert args.startdate =='20170101'
    assert args.enddate =='20170201'


def test_incorrect_startdate():
    with pytest.raises(commandline.ValidationError) as e:
        # date too early
        args = commandline.parse_args(
            [
                '--startdate', '20000101'
            ],
            include_orphans_option=True
        )

    with pytest.raises(ValueError) as e:
        args = commandline.parse_args(
            [
                '--startdate', 'GARBAGE'
            ],
            include_orphans_option=True
        )


def test_incorrect_enddate():
    with pytest.raises(commandline.ValidationError) as e:
        # date too early
        args = commandline.parse_args(
            [
                '--enddate', '20000101'
            ],
            include_orphans_option=True
        )

    with pytest.raises(ValueError) as e:
        # no date
        args = commandline.parse_args(
            [
                '--enddate', 'GARBAGE'
            ],
            include_orphans_option=True
        )


def test_end_before_start():
    with pytest.raises(commandline.ValidationError) as e:
        args = commandline.parse_args(
            [
                '--startdate', '20170801',
                '--enddate', '20170701'
            ],
            include_orphans_option=True
        )
