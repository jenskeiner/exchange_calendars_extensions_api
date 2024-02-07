import datetime as dt
from typing import Any, Union, Collection

import pandas as pd
import pytest
from pydantic import ValidationError, Field, validate_call
from typing_extensions import Annotated

from exchange_calendars_extensions.api.changes import DayType, DayProps, DayPropsWithTime, ChangeSet, Tags, TimestampLike, \
    DayPropsLike


@validate_call(config={'arbitrary_types_allowed': True})
def to_timestamp(value: TimestampLike):
    return value


@validate_call
def to_day_props(value: Annotated[Union[DayProps, DayPropsWithTime, dict], Field(discriminator='type')]):
    return value


class TestChangeSet:
    def test_empty_changeset(self):
        cs = ChangeSet()
        assert len(cs) == 0

    @pytest.mark.parametrize(["date", "props"], [
        ('2020-01-01', {'type': 'holiday', 'name': 'Holiday'}),
        ('2020-01-01', DayProps(**{'type': 'holiday', 'name': 'Holiday'})),
        (pd.Timestamp('2020-01-01'), {'type': DayType.HOLIDAY, 'name': 'Holiday'}),
        (pd.Timestamp('2020-01-01').date(), {'type': 'holiday', 'name': 'Holiday'}),
        ('2020-01-01', {'type': 'special_open', 'name': 'Special Open', 'time': '10:00'}),
        ('2020-01-01', DayPropsWithTime(**{'type': 'special_open', 'name': 'Special Open', 'time': '10:00'})),
        (pd.Timestamp('2020-01-01'), {'type': DayType.SPECIAL_OPEN, 'name': 'Special Open', 'time': '10:00:00'}),
        (pd.Timestamp('2020-01-01').date(), {'type': 'special_open', 'name': 'Special Open', 'time': dt.time(10, 0)}),
        ('2020-01-01', {'type': 'special_close', 'name': 'Special Close', 'time': '16:00'}),
        ('2020-01-01', DayPropsWithTime(**{'type': 'special_close', 'name': 'Special Close', 'time': '16:00'})),
        (pd.Timestamp('2020-01-01'), {'type': DayType.SPECIAL_CLOSE, 'name': 'Special Close', 'time': '16:00:00'}),
        (pd.Timestamp('2020-01-01').date(), {'type': 'special_close', 'name': 'Special Close', 'time': dt.time(16, 0)}),
        ('2020-01-01', {'type': 'monthly_expiry', 'name': 'Monthly Expiry'}),
        ('2020-01-01', DayProps(**{'type': 'monthly_expiry', 'name': 'Monthly Expiry'})),
        (pd.Timestamp('2020-01-01'), {'type': DayType.MONTHLY_EXPIRY, 'name': 'Monthly Expiry'}),
        (pd.Timestamp('2020-01-01').date(), {'type': 'monthly_expiry', 'name': 'Monthly Expiry'}),
        ('2020-01-01', {'type': 'quarterly_expiry', 'name': 'Quarterly Expiry'}),
        ('2020-01-01', DayProps(**{'type': 'quarterly_expiry', 'name': 'Quarterly Expiry'})),
        (pd.Timestamp('2020-01-01'), {'type': DayType.QUARTERLY_EXPIRY, 'name': 'Quarterly Expiry'}),
        (pd.Timestamp('2020-01-01').date(), {'type': 'quarterly_expiry', 'name': 'Quarterly Expiry'}),
    ])
    def test_add_valid_day(self, date: TimestampLike, props: DayPropsLike):
        # Empty changeset.
        cs = ChangeSet()

        # Add day.
        cs.add_day(date, props)

        # Check length.
        assert len(cs) == 1

        # Convert date to validated object, maybe.
        date = to_timestamp(date)

        # Convert input to validated object, maybe.
        props = to_day_props(props)

        # Get the only element from the dict.
        props0 = cs.add[date]

        # Check it's identical to the input.
        assert props0 == props

    @pytest.mark.parametrize(["date"], [
        ("2020-01-01",),
        (pd.Timestamp("2020-01-01"),),
        (pd.Timestamp("2020-01-01").date(),),
    ])
    def test_remove_day(self, date):
        cs = ChangeSet()
        cs.remove_day(date)
        assert len(cs) == 1

        # Check given day type.
        assert cs.remove == [to_timestamp(date)]

    @pytest.mark.parametrize(["date", "valid_tags"], [
        ("2020-01-01", None),
        (pd.Timestamp("2020-01-01"), None),
        (pd.Timestamp("2020-01-01").date(), None),
        ("2020-01-01", []),
        (pd.Timestamp("2020-01-01"), []),
        (pd.Timestamp("2020-01-01").date(), []),
        ("2020-01-01", ['foo']),
        (pd.Timestamp("2020-01-01"), ['foo']),
        (pd.Timestamp("2020-01-01").date(), ('foo',)),
        ("2020-01-01", {'foo', 'bar'}),
        (pd.Timestamp("2020-01-01"), ['foo', 'bar']),
        (pd.Timestamp("2020-01-01").date(), ['foo', 'bar']),
        ("2020-01-01", ['foo', 'bar', 'foo']),
        (pd.Timestamp("2020-01-01"), ['foo', 'bar', 'foo']),
        (pd.Timestamp("2020-01-01").date(), ['foo', 'bar', 'foo']),
    ])
    def test_set_valid_tags(self, date: TimestampLike, valid_tags: Tags):
        cs = ChangeSet()
        cs.set_tags(date, valid_tags)

        # Convert date to validated object, maybe.
        date = to_timestamp(date)

        if valid_tags is None or len(valid_tags) == 0:
            # Empty set of tags.
            assert len(cs) == 0
            assert date not in cs.meta
        else:
            # Non-empty set of tags. Duplicates should be removed and the result should be sorted.
            assert len(cs) == 1
            assert cs.meta[date].tags == sorted(set(valid_tags))
            assert cs.meta[date].comment is None

    @pytest.mark.parametrize(["invalid_tags"], [
        (["foo", "bar", 1],),
        (123.456,),
        ({'foo': 'bar'},),])
    @pytest.mark.parametrize(["date", "valid_tags"], [
        ("2020-01-01", None),
        (pd.Timestamp("2020-01-01"), None),
        (pd.Timestamp("2020-01-01").date(), None),
        ("2020-01-01", []),
        (pd.Timestamp("2020-01-01"), []),
        (pd.Timestamp("2020-01-01").date(), []),
        ("2020-01-01", ['foo']),
        (pd.Timestamp("2020-01-01"), ['foo']),
        (pd.Timestamp("2020-01-01").date(), ('foo',)),
        ("2020-01-01", {'foo', 'bar'}),
        (pd.Timestamp("2020-01-01"), ['foo', 'bar']),
        (pd.Timestamp("2020-01-01").date(), ['foo', 'bar']),
        ("2020-01-01", ['foo', 'bar', 'foo']),
        (pd.Timestamp("2020-01-01"), ['foo', 'bar', 'foo']),
        (pd.Timestamp("2020-01-01").date(), ['foo', 'bar', 'foo']),
    ])
    def test_set_invalid_tags(self, date: TimestampLike, valid_tags: Tags, invalid_tags: Any):
        # Fresh changeset.
        cs = ChangeSet()
        
        # Set valid tags.
        cs.set_tags(date, valid_tags)
        
        # Set invalid tags.
        with pytest.raises(ValueError):
            cs.set_tags(date, invalid_tags)
            
    @pytest.mark.parametrize(["include_tags"], [(True,), (False,)], ids=['include_tags', 'exclude_meta'])
    @pytest.mark.parametrize(["date", "props"], [
        ('2020-01-01', {'type': 'holiday', 'name': 'Holiday'}),
        (pd.Timestamp('2020-01-01'), {'type': 'holiday', 'name': 'Holiday'}),
        (pd.Timestamp('2020-01-01').date(), {'type': 'holiday', 'name': 'Holiday'}),
        ('2020-01-01', {'type': 'special_open', 'name': 'Special Open', 'time': '10:00'}),
        (pd.Timestamp('2020-01-01'), {'type': 'special_open', 'name': 'Special Open', 'time': '10:00:00'}),
        (pd.Timestamp('2020-01-01').date(), {'type': 'special_open', 'name': 'Special Open', 'time': dt.time(10, 0)}),
        ('2020-01-01', {'type': 'special_close', 'name': 'Special Close', 'time': '16:00'}),
        (pd.Timestamp('2020-01-01'), {'type': 'special_close', 'name': 'Special Close', 'time': '16:00:00'}),
        (pd.Timestamp('2020-01-01').date(), {'type': 'special_close', 'name': 'Special Close', 'time': dt.time(16, 0)}),
        ('2020-01-01', {'type': 'monthly_expiry', 'name': 'Monthly Expiry'}),
        (pd.Timestamp('2020-01-01'), {'type': 'monthly_expiry', 'name': 'Monthly Expiry'}),
        (pd.Timestamp('2020-01-01').date(), {'type': 'monthly_expiry', 'name': 'Monthly Expiry'}),
        ('2020-01-01', {'type': 'quarterly_expiry', 'name': 'Quarterly Expiry'}),
        (pd.Timestamp('2020-01-01'), {'type': 'quarterly_expiry', 'name': 'Quarterly Expiry'}),
        (pd.Timestamp('2020-01-01').date(), {'type': 'quarterly_expiry', 'name': 'Quarterly Expiry'}),
    ])
    def test_clear_day(self, date: TimestampLike, props: DayPropsLike, include_tags: bool):
        # Empty changeset.
        cs = ChangeSet()

        # Add day.
        cs.add_day(date, props)
        assert len(cs) == 1

        # Add tags for same day.
        cs.set_tags(date, ['foo', 'bar'])
        assert len(cs) == 2

        # Clear day.
        cs.clear_day(date, include_meta=include_tags)
        assert len(cs) == 0 if include_tags else 1

        # Empty changeset.
        cs = ChangeSet()

        # Remove day.
        cs.remove_day(date)
        assert len(cs) == 1

        # Add tags for same day.
        cs.set_tags(date, ['foo', 'bar'])
        assert len(cs) == 2

        # Clear day.
        cs.clear_day(date, include_meta=include_tags)
        assert len(cs) == 0 if include_tags else 1

    @pytest.mark.parametrize(["include_meta"], [(True,), (False,)], ids=['include_meta', 'exclude_meta'])
    def test_clear(self, include_meta: bool):
        cs = ChangeSet()
        cs.add_day('2020-01-01', {'type': 'holiday', 'name': 'Holiday'})
        cs.add_day('2020-01-02', {'type': 'special_open', 'name': 'Special Open', 'time': '10:00'})
        cs.add_day('2020-01-03', {'type': 'special_close', 'name': 'Special Close', 'time': '16:00'})
        cs.add_day('2020-01-04', {'type': 'monthly_expiry', 'name': 'Monthly Expiry'})
        cs.add_day('2020-01-05', {'type': 'quarterly_expiry', 'name': 'Quarterly Expiry'})
        cs.remove_day('2020-01-06')
        cs.remove_day('2020-01-07')
        cs.remove_day('2020-01-08')
        cs.remove_day('2020-01-09')
        cs.remove_day('2020-01-10')
        cs.set_tags('2020-01-01', ['foo', 'bar'])
        cs.set_tags('2020-01-02', ['foo', 'bar'])
        cs.set_tags('2020-01-03', ['foo', 'bar'])
        cs.set_tags('2020-01-04', ['foo', 'bar'])
        cs.set_tags('2020-01-05', ['foo', 'bar'])
        cs.set_tags('2020-01-06', ['foo', 'bar'])
        cs.set_tags('2020-01-07', ['foo', 'bar'])
        cs.set_tags('2020-01-08', ['foo', 'bar'])
        cs.set_tags('2020-01-09', ['foo', 'bar'])
        cs.set_tags('2020-01-10', ['foo', 'bar'])

        assert len(cs) == 20

        cs.clear(include_meta=include_meta)

        if include_meta:
            assert not cs
            assert cs.add == dict()
            assert cs.remove == []
            assert cs.meta == dict()
        else:
            assert len(cs) == 10
            assert cs.add == dict()
            assert cs.remove == []
            assert len(cs.meta) == 10

    @pytest.mark.parametrize(['date', 'props'], [
        ('2020-01-01', {'type': 'holiday', 'name': 'Holiday'},),
        ('2020-01-01', {'type': 'special_open', 'name': 'Special Open', 'time': '10:00'},),
        ('2020-01-01', {'type': 'special_close', 'name': 'Special Close', 'time': '16:00'},),
        ('2020-01-01', {'type': 'monthly_expiry', 'name': 'Monthly Expiry'},),
        ('2020-01-01', {'type': 'quarterly_expiry', 'name': 'Quarterly Expiry'},),
    ])
    def test_add_remove_day_for_same_day_type(self, date: TimestampLike, props: DayPropsLike):
        cs = ChangeSet()
        cs.add_day(date, props)
        cs.remove_day(date)
        assert cs
        assert len(cs) == 2
        assert cs.add == {to_timestamp(date): to_day_props(props)}
        assert cs.remove == [to_timestamp(date)]
        assert cs.meta == dict()

    def test_add_same_day_twice(self):
        cs = ChangeSet()
        date = '2020-01-01'
        props = {'type': 'holiday', 'name': 'Holiday'}
        props_alt = {'type': 'special_open', 'name': 'Special Open', 'time': '10:00'}
        cs.add_day(date, props)
        with pytest.raises(ValueError):
            cs.add_day(date, props_alt)
        assert cs
        assert len(cs) == 1
        assert cs.add == {to_timestamp(date): to_day_props(props)}
        assert cs.remove == []
        assert cs.meta == dict()

    @pytest.mark.parametrize(['d', 'cs'], [
        ({'add': {'2020-01-01': {'type': 'holiday', 'name': 'Holiday'}}},
         ChangeSet().add_day('2020-01-01', {'type': 'holiday', 'name': 'Holiday'})),
        ({'add': {'2020-01-01': {'type': 'special_open', 'name': 'Special Open', 'time': '10:00'}}},
         ChangeSet().add_day('2020-01-01', {'type': 'special_open', 'name': 'Special Open', 'time': '10:00'})),
        ({'add': {'2020-01-01': {'type': 'special_close', 'name': 'Special Close', 'time': '16:00'}}},
         ChangeSet().add_day('2020-01-01', {'type': 'special_close', 'name': 'Special Close', 'time': '16:00'})),
        ({'add': {'2020-01-01': {'type': 'monthly_expiry', 'name': 'Monthly Expiry'}}},
         ChangeSet().add_day('2020-01-01', {'type': 'monthly_expiry', 'name': 'Monthly Expiry'})),
        ({'add': {'2020-01-01': {'type': 'quarterly_expiry', 'name': 'Quarterly Expiry'}}},
         ChangeSet().add_day('2020-01-01',{'type': 'quarterly_expiry', 'name': 'Quarterly Expiry'})),
        ({'remove': ['2020-01-01']},
         ChangeSet().remove_day('2020-01-01')),
        ({'meta': {'2020-01-01': {'tags': ['foo', 'bar']}}},
         ChangeSet().set_tags('2020-01-01', ['foo', 'bar'])),
        ({
             'add': {
                 '2020-01-01': {'type': 'holiday', 'name': 'Holiday'},
                 '2020-02-01': {'type': 'special_open', 'name': 'Special Open', 'time': '10:00'},
                 '2020-03-01': {'type': 'special_close', 'name': 'Special Close', 'time': '16:00'},
                 '2020-04-01': {'type': 'monthly_expiry', 'name': 'Monthly Expiry'},
                 '2020-05-01': {'type': 'quarterly_expiry', 'name': 'Quarterly Expiry'},
             },
             'remove': ['2020-01-02','2020-02-02','2020-03-02','2020-04-02','2020-05-02'],
             'meta': {
                    '2020-01-01': {'tags': ['foo', 'bar']},
                    '2020-02-01': {'tags': ['foo', 'bar']},
                    '2020-03-01': {'tags': ['foo', 'bar']},
                    '2020-04-01': {'tags': ['foo', 'bar']},
                    '2020-05-01': {'tags': ['foo', 'bar']},
                    '2020-01-02': {'tags': ['foo', 'bar']},
                    '2020-02-02': {'tags': ['foo', 'bar']},
                    '2020-03-02': {'tags': ['foo', 'bar']},
                    '2020-04-02': {'tags': ['foo', 'bar']},
                    '2020-05-02': {'tags': ['foo', 'bar']},
                }
         },
         ChangeSet()
         .add_day('2020-01-01', {'type': 'holiday', 'name': 'Holiday'})
         .set_tags('2020-01-01', ['foo', 'bar'])
         .remove_day('2020-01-02')
         .set_tags('2020-01-02', ['foo', 'bar'])
         .add_day('2020-02-01', {'type': 'special_open', 'name': 'Special Open', 'time': '10:00'})
         .set_tags('2020-02-01', ['foo', 'bar'])
         .remove_day('2020-02-02')
         .set_tags('2020-02-02', ['foo', 'bar'])
         .add_day('2020-03-01', {'type': 'special_close', 'name': 'Special Close', 'time': '16:00'})
         .set_tags('2020-03-01', ['foo', 'bar'])
         .remove_day('2020-03-02')
         .set_tags('2020-03-02', ['foo', 'bar'])
         .add_day('2020-04-01', {'type': 'monthly_expiry', 'name': 'Monthly Expiry'})
         .set_tags('2020-04-01', ['foo', 'bar'])
         .remove_day('2020-04-02')
         .set_tags('2020-04-02', ['foo', 'bar'])
         .add_day('2020-05-01', {'type': 'quarterly_expiry', 'name': 'Quarterly Expiry'})
         .set_tags('2020-05-01', ['foo', 'bar'])
         .remove_day('2020-05-02')
         .set_tags('2020-05-02', ['foo', 'bar']))
    ])
    def test_changeset_from_valid_non_empty_dict(self, d: dict, cs: ChangeSet):
        cs0 = ChangeSet.model_validate(d)
        assert cs0 == cs

    @pytest.mark.parametrize(['d'], [
        # Invalid day type.
        ({'add': {'2020-01-01': {'type': 'foo', 'name': 'Holiday'}}},),
        # Invalid date.
        ({'add': {'foo': {'type': 'holiday', 'name': 'Holiday'}}},),
        ({'add': {'foo': {'type': 'monthly_expiry', 'name': 'Holiday'}}},),
        ({'add': {'foo': {'type': 'quarterly_expiry', 'name': 'Holiday'}}},),
        # # Invalid value.
        ({'add': {'2020-01-01': {'type': 'holiday', 'foo': 'Holiday'}}},),
        ({'add': {'2020-01-01': {'type': 'holiday', 'name': 'Holiday', 'time': '10:00'}}},),
        ({'add': {'2020-01-01': {'type': 'holiday', 'name': 'Holiday', 'foo': 'bar'}}},),
        ({'add': {'2020-01-01': {'type': 'monthly_expiry', 'foo': 'Monthly Expiry'}}},),
        ({'add': {'2020-01-01': {'type': 'quarterly_expiry', 'foo': 'Quarterly Expiry'}}},),
        # Invalid day type.
        ({'add': {'2020-01-01': {'type': 'foo', 'name': 'Special Open', 'time': '10:00'}}},),
        ({'add': {'2020-01-01': {'type': 'foo', 'name': 'Special Close', 'time': '10:00'}}},),
        # Invalid date.
        ({'add': {'foo': {'type': 'special_open', 'name': 'Special Open', 'time': '10:00'}}},),
        ({'add': {'foo': {'type': 'special_close', 'name': 'Special Close', 'time': '10:00'}}},),
        # Invalid value key.
        ({'add': {'2020-01-01': {'type': 'special_open', 'foo': 'Special Open', 'time': '10:00'}}},),
        ({'add': {'2020-01-01': {'type': 'special_open', 'name': 'Special Open', 'foo': '10:00'}}},),
        ({'add': {'2020-01-01': {'type': 'special_close', 'foo': 'Special Close', 'time': '10:00'}}},),
        ({'add': {'2020-01-01': {'type': 'special_close', 'name': 'Special Close', 'foo': '10:00'}}},),
        # Invalid date.
        ({'remove': ['2020-01-01', 'foo']},),
        # Invalid date.
        ({'tags': {'2020-01-01': ['foo', 'bar'], 'foo': ['foo', 'bar']}},),
        # Invalid tag values.
        ({'tags': {'2020-01-01': ['foo', 'bar', 'foo', 1, 2, 3]}},),
    ])
    def test_changeset_from_invalid_dict(self, d: dict):
        with pytest.raises(ValidationError):
            ChangeSet.model_validate(d)

    @pytest.mark.parametrize(['date', 'props1', 'props2'], [
        # Same day added twice for different day types.
        ('2020-01-01',
         {'type': 'holiday', 'name': 'Holiday'},
         {'type': 'special_open', 'name': 'Special Open', 'time': '10:00'}),
        ('2020-01-01',
         {'type': 'holiday', 'name': 'Holiday'},
         {'type': 'special_close', 'name': 'Special Close', 'time': '10:00'}),
        ('2020-01-01',
         {'type': 'holiday', 'name': 'Holiday'},
         {'type': 'monthly_expiry', 'name': 'Monthly Expiry'}),
        ('2020-01-01',
         {'type': 'holiday', 'name': 'Holiday'},
         {'type': 'quarterly_expiry', 'name': 'Quarterly Expiry'}),
    ])
    def test_changeset_from_inconsistent_dict(self, date: TimestampLike, props1: DayPropsLike, props2: DayPropsLike):
        # Empty changeset.
        cs = ChangeSet()

        # Add day.
        cs.add_day(date, props1)

        with pytest.raises(ValueError):
            cs.add_day(date, props2)

    @pytest.mark.parametrize(["include_meta"], [(True,), (False,)], ids=['include_meta', 'exclude_meta'])
    def test_all_days(self, include_meta: bool):
        cs = ChangeSet(
            add={
                '2020-01-01': {'type': 'holiday', 'name': 'Holiday'},
                '2020-02-01': {'type': 'special_open', 'name': 'Special Open', 'time': '10:00'},
                '2020-03-01': {'type': 'special_close', 'name': 'Special Close', 'time': '16:00'},
                '2020-04-01': {'type': 'monthly_expiry', 'name': 'Monthly Expiry'},
                '2020-05-01': {'type': 'quarterly_expiry', 'name': 'Quarterly Expiry'},
            },
            remove=['2020-01-02', '2020-02-02', '2020-03-02', '2020-04-02', '2020-05-02'],
            meta={
                '2020-01-03': {'tags': ['foo', 'bar']},
                '2020-02-03': {'tags': ['foo', 'bar']},
                '2020-03-03': {'tags': ['foo', 'bar']},
                '2020-04-03': {'tags': ['foo', 'bar']},
                '2020-05-03': {'tags': ['foo', 'bar']},
            }
        )
        assert cs.all_days(include_meta=include_meta) == tuple(sorted(map(to_timestamp, ['2020-01-01', '2020-01-02',
                                                                                         '2020-02-01', '2020-02-02',
                                                                                         '2020-03-01', '2020-03-02',
                                                                                         '2020-04-01', '2020-04-02',
                                                                                         '2020-05-01',
                                                                                         '2020-05-02', ] + (
                                                                              ['2020-01-3', '2020-02-03', '2020-03-03',
                                                                               '2020-04-03',
                                                                               '2020-05-03'] if include_meta else []))))
