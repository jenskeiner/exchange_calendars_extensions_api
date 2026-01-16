import datetime as dt
from typing import Any, Union
from collections.abc import Collection

import pandas as pd
import pytest
from pydantic import ValidationError, TypeAdapter

from src.exchange_calendars_extensions.api.changes import (
    DayType,
    DayProps,
    DayPropsWithTime,
    ChangeSet,
    Tags,
    DateLike,
    DayPropsLike,
    DayMeta,
)

# Validator for TimestampLike.
TimeStampValidator = TypeAdapter(DateLike, config=dict(arbitrary_types_allowed=True))

# Validator for DayPropsLike.
DayPropsValidator = TypeAdapter(DayPropsLike, config=dict(arbitrary_types_allowed=True))

# Validator for DayMeta.
DayMetaValidator = TypeAdapter(
    Union[DayMeta, None], config=dict(arbitrary_types_allowed=True)
)


def to_args(values: Collection):
    """
    Convert a collection of values to a list of tuples, each containing a single value.

    Helpful when using @pytest.mark.parametrize.

    Parameters
    ----------
    values : Collection
        The collection of values.

    Returns
    -------
    list
        A list of tuples, each containing a single value.

    """
    return list(map(lambda x: (x,), values))


# Set of valid tags.
VALID_TAGS = [
    None,
    [],
    ["foo"],
    ["foo", "bar"],
    ["foo", "bar", "foo"],
    ("foo", "bar", "foo"),
    {"foo", "bar"},
]

# Set of invalid tags.
INVALID_TAGS = [123, 123.456, "foo", {"foo": "bar"}, ["foo", "bar", 1]]

# Valid comments.
VALID_COMMENTS = [None, "", "This is a comment."]

# Invalid comments.
INVALID_COMMENTS = [123, 123.456, {"foo": "bar"}, ["foo", "bar", 1]]

VALID_META = [
    None,
    DayMeta(),
    {"tags": ["foo", "bar"]},
    DayMeta(tags=["foo", "bar"]),
    {"tags": ["foo", "bar"], "comment": "This is a comment."},
    DayMeta(tags=["foo", "bar"], comment="This is a comment."),
    {"comment": "This is a comment."},
    DayMeta(comment="This is a comment."),
]

# Set of valid dates.
VALID_DATES = [
    "2020-01-01",
    pd.Timestamp("2020-01-01"),
    pd.Timestamp("2020-01-01").date(),
    "2020-01",
    1577833200,
]

# Set of invalid dates.
INVALID_DATES = ["2020-001", "#2020", "2020:01:01", None, {"foo": "bar"}]

# Set of valid day properties.
VALID_PROPS = [
    {"type": "holiday", "name": "Holiday"},
    DayProps(**{"type": "holiday", "name": "Holiday"}),
    {"type": "special_open", "name": "Special Open", "time": "10:00"},
    DayPropsWithTime(
        **{"type": "special_open", "name": "Special Open", "time": "10:00"}
    ),
    {"type": DayType.SPECIAL_OPEN, "name": "Special Open", "time": "10:00:00"},
    {"type": "special_open", "name": "Special Open", "time": dt.time(10, 0)},
    {"type": "special_close", "name": "Special Close", "time": "16:00"},
    DayPropsWithTime(
        **{"type": "special_close", "name": "Special Close", "time": "16:00"}
    ),
    {"type": DayType.SPECIAL_CLOSE, "name": "Special Close", "time": "16:00:00"},
    {"type": "special_close", "name": "Special Close", "time": dt.time(16, 0)},
    {"type": "monthly_expiry", "name": "Monthly Expiry"},
    DayProps(**{"type": "monthly_expiry", "name": "Monthly Expiry"}),
    {"type": DayType.MONTHLY_EXPIRY, "name": "Monthly Expiry"},
    {"type": "monthly_expiry", "name": "Monthly Expiry"},
    {"type": "quarterly_expiry", "name": "Quarterly Expiry"},
    DayProps(**{"type": "quarterly_expiry", "name": "Quarterly Expiry"}),
    {"type": DayType.QUARTERLY_EXPIRY, "name": "Quarterly Expiry"},
    {"type": "quarterly_expiry", "name": "Quarterly Expiry"},
]

# Set of invalid day properties.
INVALID_PROPS = [
    # Invalid day type.
    {"type": "foo", "name": "Holiday"},
    {"type": "foo", "name": "Special Open", "time": "10:00"},
    {"type": "foo", "name": "Special Close", "time": "10:00"},
    # Other invalid properties.
    {"type": "holiday", "foo": "Holiday"},
    {"type": "holiday", "name": "Holiday", "time": "10:00"},
    {"type": "holiday", "name": "Holiday", "foo": "bar"},
    {"type": "monthly_expiry", "foo": "Monthly Expiry"},
    {"type": "quarterly_expiry", "foo": "Quarterly Expiry"},
    {"type": "special_open", "foo": "Special Open", "time": "10:00"},
    {"type": "special_open", "name": "Special Open", "foo": "10:00"},
    {"type": "special_close", "foo": "Special Close", "time": "10:00"},
    {"type": "special_close", "name": "Special Close", "foo": "10:00"},
]


class TestChangeSet:
    def test_empty_changeset(self):
        cs = ChangeSet()
        assert len(cs) == 0
        assert not cs
        assert cs.add == dict()
        assert cs.remove == []
        assert cs.meta == dict()

    # add_day

    @pytest.mark.parametrize(["date"], to_args(VALID_DATES))
    @pytest.mark.parametrize(["props"], to_args(VALID_PROPS))
    def test_add_day(self, date: DateLike, props: DayPropsLike):
        # Empty changeset.
        cs = ChangeSet()

        # Add day.
        cs.add_day(date, props)

        # Check length.
        assert len(cs) == 1

        # Convert date to validated object, maybe.
        date = TimeStampValidator.validate_python(date)

        # Convert input to validated object, maybe.
        props = DayPropsValidator.validate_python(props)

        # Get the only element from the dict.
        props0 = cs.add[date]

        # Check it's identical to the input.
        assert props0 == props

        # Check the rest of the changeset.
        assert cs.remove == []
        assert cs.meta == dict()

    @pytest.mark.parametrize(["date"], to_args(INVALID_DATES))
    def test_add_day_invalid_date(self, date: Any):
        # Fresh changeset.
        cs = ChangeSet()

        # Add day.
        # noinspection PyTypeChecker
        with pytest.raises((ValidationError, TypeError)):
            cs.add_day(date, {"type": "holiday", "name": "Holiday"})

    @pytest.mark.parametrize(["props"], to_args(INVALID_PROPS))
    def test_add_day_invalid_props(self, props: Any):
        # Fresh changeset.
        cs = ChangeSet()

        # Add day.
        # noinspection PyTypeChecker
        with pytest.raises((ValidationError, TypeError)):
            cs.add_day("2020-01-01", props)

    # remove_day

    @pytest.mark.parametrize(["date"], to_args(VALID_DATES))
    def test_remove_day(self, date):
        cs = ChangeSet()
        cs.remove_day(date)
        assert len(cs) == 1

        # Check given day type.
        assert cs.remove == [TimeStampValidator.validate_python(date)]

    @pytest.mark.parametrize(["date"], to_args(INVALID_DATES))
    def test_remove_day_invalid_date(self, date: Any):
        # Fresh changeset.
        cs = ChangeSet()

        # Remove day.
        # noinspection PyTypeChecker
        with pytest.raises((ValidationError, TypeError)):
            cs.remove_day(date)

    # set_tags

    @pytest.mark.parametrize(["date"], to_args(VALID_DATES))
    @pytest.mark.parametrize(["tags"], to_args(VALID_TAGS))
    def test_set_tags(self, date: DateLike, tags: Tags):
        cs = ChangeSet()
        cs.set_tags(date, tags)

        # Ensure timestamp.
        ts = TimeStampValidator.validate_python(date)

        if tags is None or len(tags) == 0:
            # Empty set of tags.
            assert len(cs) == 0
            assert ts not in cs.meta
        else:
            # Non-empty set of tags. Duplicates should be removed and the result should be sorted.
            assert len(cs) == 1
            assert cs.meta[ts].tags == sorted(set(tags))
            assert cs.meta[ts].comment is None

    @pytest.mark.parametrize(["date"], to_args(INVALID_DATES))
    def test_set_tags_invalid_date(self, date: Any):
        # Fresh changeset.
        cs = ChangeSet()

        # Set tags.
        # noinspection PyTypeChecker
        with pytest.raises((ValidationError, TypeError)):
            cs.set_tags(date, ["foo", "bar"])

    @pytest.mark.parametrize(["tags"], to_args(INVALID_TAGS))
    def test_set_tags_invalid_tags(self, tags: Any):
        # Fresh changeset.
        cs = ChangeSet()

        # Set invalid tags.
        with pytest.raises(ValueError):
            cs.set_tags("2020-01-01", tags)

    def test_set_tags_to_none(self):
        d: str = "2020-01-01"
        ts: pd.Timestamp = TimeStampValidator.validate_python(d)

        # Fresh changeset.
        cs = ChangeSet()

        # Set tags.
        cs.set_tags(d, ["foo", "bar"])

        # Set comment.
        cs.set_comment(d, "This is a comment.")

        assert len(cs) == 1
        assert cs.add == dict()
        assert cs.remove == []
        assert ts in cs.meta
        assert cs.meta[ts].tags == ["bar", "foo"]  # Tags get sorted alphabetically.
        assert cs.meta[ts].comment == "This is a comment."

        # Set tags to None.
        cs.set_tags(d, None)

        assert len(cs) == 1
        assert cs.add == dict()
        assert cs.remove == []
        assert ts in cs.meta
        assert (
            cs.meta[ts].tags == []
        )  # Setting tags to None should actually set the tags to the empty list.
        assert cs.meta[ts].comment == "This is a comment."

    # set_comment

    @pytest.mark.parametrize(["date"], to_args(VALID_DATES))
    @pytest.mark.parametrize(["comment"], to_args(VALID_COMMENTS))
    def test_set_comment(self, date: DateLike, comment: Union[str, None]):
        cs = ChangeSet()
        cs.set_comment(date, comment)

        # Convert date to validated object, maybe.
        ts = TimeStampValidator.validate_python(date)

        if comment is None or len(comment) == 0:
            # Empty comment.
            assert len(cs) == 0
            assert ts not in cs.meta
        else:
            # Non-empty comment.
            assert len(cs) == 1
            assert cs.meta[ts].tags == []
            assert cs.meta[ts].comment == comment

    @pytest.mark.parametrize(["date"], to_args(INVALID_DATES))
    def test_set_comment_invalid_date(self, date: Any):
        # Fresh changeset.
        cs = ChangeSet()

        # Set comment.
        # noinspection PyTypeChecker
        with pytest.raises((ValidationError, TypeError)):
            cs.set_comment(date, "This is a comment.")

    @pytest.mark.parametrize(["comment"], to_args(INVALID_COMMENTS))
    def test_set_comment_invalid_comment(self, comment: Any):
        # Fresh changeset.
        cs = ChangeSet()

        # Set invalid comment.
        with pytest.raises(ValueError):
            cs.set_comment("2020-01-01", comment)

    def test_set_comment_to_empty_string(self):
        d: str = "2020-01-01"
        ts: pd.Timestamp = TimeStampValidator.validate_python(d)

        # Fresh changeset.
        cs = ChangeSet()

        # Set tags.
        cs.set_tags(d, ["foo", "bar"])

        # Set comment.
        cs.set_comment(d, "This is a comment.")

        assert len(cs) == 1
        assert cs.add == dict()
        assert cs.remove == []
        assert ts in cs.meta
        assert cs.meta[ts].tags == ["bar", "foo"]
        assert cs.meta[ts].comment == "This is a comment."

        # Set comment to empty string.
        cs.set_comment(d, "")

        assert len(cs) == 1
        assert cs.add == dict()
        assert cs.remove == []
        assert ts in cs.meta
        assert cs.meta[ts].tags == ["bar", "foo"]
        assert cs.meta[ts].comment is None  # Empty string should convert to None.

    # set_meta

    @pytest.mark.parametrize(["date"], to_args(VALID_DATES))
    @pytest.mark.parametrize(["meta"], to_args(VALID_META))
    def test_set_meta(self, date: DateLike, meta: Any):
        cs = ChangeSet()
        cs.set_meta(date, meta)

        # Ensure timestamp.
        ts = TimeStampValidator.validate_python(date)

        # Convert input to validated object.
        meta = DayMetaValidator.validate_python(meta)

        if meta is None or len(meta) == 0:
            assert len(cs) == 0
            assert cs.add == dict()
            assert cs.remove == []
            assert cs.meta == dict()
        else:
            assert len(cs) == 1
            assert cs.add == dict()
            assert cs.remove == []
            assert cs.meta == {ts: meta}

    # clear_day

    @pytest.mark.parametrize(
        ["include_tags"], [(True,), (False,)], ids=["include_tags", "exclude_meta"]
    )
    @pytest.mark.parametrize(["date"], to_args(VALID_DATES))
    @pytest.mark.parametrize(["props"], to_args(VALID_PROPS))
    def test_clear_day(self, date: DateLike, props: DayPropsLike, include_tags: bool):
        # Empty changeset.
        cs = ChangeSet()

        # Add day.
        cs.add_day(date, props)
        assert len(cs) == 1

        # Add tags for same day.
        cs.set_tags(date, ["foo", "bar"])
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
        cs.set_tags(date, ["foo", "bar"])
        assert len(cs) == 2

        # Clear day.
        cs.clear_day(date, include_meta=include_tags)
        assert len(cs) == 0 if include_tags else 1

    @pytest.mark.parametrize(["date"], to_args(INVALID_DATES))
    def test_clear_day_invalid_date(self, date: Any):
        # Fresh changeset.
        cs = ChangeSet()

        # Clear day.
        # noinspection PyTypeChecker
        with pytest.raises((ValidationError, TypeError)):
            cs.clear_day(date)

    # clear

    @pytest.mark.parametrize(
        ["include_meta"], [(True,), (False,)], ids=["include_meta", "exclude_meta"]
    )
    def test_clear(self, include_meta: bool):
        cs = ChangeSet()
        cs.add_day("2020-01-01", {"type": "holiday", "name": "Holiday"})
        cs.add_day(
            "2020-01-02",
            {"type": "special_open", "name": "Special Open", "time": "10:00"},
        )
        cs.add_day(
            "2020-01-03",
            {"type": "special_close", "name": "Special Close", "time": "16:00"},
        )
        cs.add_day("2020-01-04", {"type": "monthly_expiry", "name": "Monthly Expiry"})
        cs.add_day(
            "2020-01-05", {"type": "quarterly_expiry", "name": "Quarterly Expiry"}
        )
        cs.remove_day("2020-01-06")
        cs.remove_day("2020-01-07")
        cs.remove_day("2020-01-08")
        cs.remove_day("2020-01-09")
        cs.remove_day("2020-01-10")
        cs.set_tags("2020-01-01", ["foo", "bar"])
        cs.set_tags("2020-01-02", ["foo", "bar"])
        cs.set_tags("2020-01-03", ["foo", "bar"])
        cs.set_tags("2020-01-04", ["foo", "bar"])
        cs.set_tags("2020-01-05", ["foo", "bar"])
        cs.set_tags("2020-01-06", ["foo", "bar"])
        cs.set_tags("2020-01-07", ["foo", "bar"])
        cs.set_tags("2020-01-08", ["foo", "bar"])
        cs.set_tags("2020-01-09", ["foo", "bar"])
        cs.set_tags("2020-01-10", ["foo", "bar"])

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

    @pytest.mark.parametrize(["date"], to_args(VALID_DATES))
    @pytest.mark.parametrize(["props"], to_args(VALID_PROPS))
    def test_add_remove_day_for_same_day_type(
        self, date: DateLike, props: DayPropsLike
    ):
        cs = ChangeSet()
        cs.add_day(date, props)
        cs.remove_day(date)
        assert cs
        assert len(cs) == 2
        assert cs.add == {
            TimeStampValidator.validate_python(date): DayPropsValidator.validate_python(
                props
            )
        }
        assert cs.remove == [TimeStampValidator.validate_python(date)]
        assert cs.meta == dict()

    def test_add_same_day_twice(self):
        cs = ChangeSet()
        date = "2020-01-01"
        props = {"type": "holiday", "name": "Holiday"}
        props_alt = {"type": "special_open", "name": "Special Open", "time": "10:00"}
        cs.add_day(date, props)
        with pytest.raises(ValueError):
            cs.add_day(date, props_alt)
        assert cs
        assert len(cs) == 1
        assert cs.add == {
            TimeStampValidator.validate_python(date): DayPropsValidator.validate_python(
                props
            )
        }
        assert cs.remove == []
        assert cs.meta == dict()

    @pytest.mark.parametrize(
        ["d", "cs"],
        [
            (
                {"add": {"2020-01-01": {"type": "holiday", "name": "Holiday"}}},
                ChangeSet().add_day(
                    "2020-01-01", {"type": "holiday", "name": "Holiday"}
                ),
            ),
            (
                {
                    "add": {
                        "2020-01-01": {
                            "type": "special_open",
                            "name": "Special Open",
                            "time": "10:00",
                        }
                    }
                },
                ChangeSet().add_day(
                    "2020-01-01",
                    {"type": "special_open", "name": "Special Open", "time": "10:00"},
                ),
            ),
            (
                {
                    "add": {
                        "2020-01-01": {
                            "type": "special_close",
                            "name": "Special Close",
                            "time": "16:00",
                        }
                    }
                },
                ChangeSet().add_day(
                    "2020-01-01",
                    {"type": "special_close", "name": "Special Close", "time": "16:00"},
                ),
            ),
            (
                {
                    "add": {
                        "2020-01-01": {
                            "type": "monthly_expiry",
                            "name": "Monthly Expiry",
                        }
                    }
                },
                ChangeSet().add_day(
                    "2020-01-01", {"type": "monthly_expiry", "name": "Monthly Expiry"}
                ),
            ),
            (
                {
                    "add": {
                        "2020-01-01": {
                            "type": "quarterly_expiry",
                            "name": "Quarterly Expiry",
                        }
                    }
                },
                ChangeSet().add_day(
                    "2020-01-01",
                    {"type": "quarterly_expiry", "name": "Quarterly Expiry"},
                ),
            ),
            ({"remove": ["2020-01-01"]}, ChangeSet().remove_day("2020-01-01")),
            (
                {"meta": {"2020-01-01": {"tags": ["foo", "bar"]}}},
                ChangeSet().set_tags("2020-01-01", ["foo", "bar"]),
            ),
            (
                {
                    "add": {
                        "2020-01-01": {"type": "holiday", "name": "Holiday"},
                        "2020-02-01": {
                            "type": "special_open",
                            "name": "Special Open",
                            "time": "10:00",
                        },
                        "2020-03-01": {
                            "type": "special_close",
                            "name": "Special Close",
                            "time": "16:00",
                        },
                        "2020-04-01": {
                            "type": "monthly_expiry",
                            "name": "Monthly Expiry",
                        },
                        "2020-05-01": {
                            "type": "quarterly_expiry",
                            "name": "Quarterly Expiry",
                        },
                    },
                    "remove": [
                        "2020-01-02",
                        "2020-02-02",
                        "2020-03-02",
                        "2020-04-02",
                        "2020-05-02",
                    ],
                    "meta": {
                        "2020-01-01": {"tags": ["foo", "bar"]},
                        "2020-02-01": {"tags": ["foo", "bar"]},
                        "2020-03-01": {
                            "tags": ["foo", "bar"],
                            "comment": "This is a comment.",
                        },
                        "2020-04-01": {
                            "tags": ["foo", "bar"],
                            "comment": "This is a comment.",
                        },
                        "2020-05-01": {"comment": "This is a comment."},
                        "2020-01-02": {"comment": "This is a comment."},
                        "2020-02-02": {"tags": ["foo", "bar"]},
                        "2020-03-02": {"tags": ["foo", "bar"]},
                        "2020-04-02": {"tags": ["foo", "bar"]},
                        "2020-05-02": {"tags": ["foo", "bar"]},
                    },
                },
                ChangeSet()
                .add_day("2020-01-01", {"type": "holiday", "name": "Holiday"})
                .set_tags("2020-01-01", ["foo", "bar"])
                .remove_day("2020-01-02")
                .set_comment("2020-01-02", "This is a comment.")
                .add_day(
                    "2020-02-01",
                    {"type": "special_open", "name": "Special Open", "time": "10:00"},
                )
                .set_tags("2020-02-01", ["foo", "bar"])
                .remove_day("2020-02-02")
                .set_tags("2020-02-02", ["foo", "bar"])
                .add_day(
                    "2020-03-01",
                    {"type": "special_close", "name": "Special Close", "time": "16:00"},
                )
                .set_tags("2020-03-01", ["foo", "bar"])
                .set_comment("2020-03-01", "This is a comment.")
                .remove_day("2020-03-02")
                .set_tags("2020-03-02", ["foo", "bar"])
                .add_day(
                    "2020-04-01", {"type": "monthly_expiry", "name": "Monthly Expiry"}
                )
                .set_tags("2020-04-01", ["foo", "bar"])
                .set_comment("2020-04-01", "This is a comment.")
                .remove_day("2020-04-02")
                .set_tags("2020-04-02", ["foo", "bar"])
                .add_day(
                    "2020-05-01",
                    {"type": "quarterly_expiry", "name": "Quarterly Expiry"},
                )
                .set_comment("2020-05-01", "This is a comment.")
                .remove_day("2020-05-02")
                .set_tags("2020-05-02", ["foo", "bar"]),
            ),
        ],
    )
    def test_changeset_from_valid_non_empty_dict(self, d: dict, cs: ChangeSet):
        cs0 = ChangeSet.model_validate(d)
        assert cs0 == cs

    @pytest.mark.parametrize(
        ["d"],
        [
            # Invalid day type.
            ({"add": {"2020-01-01": {"type": "foo", "name": "Holiday"}}},),
            # Invalid date.
            ({"add": {"foo": {"type": "holiday", "name": "Holiday"}}},),
            ({"add": {"foo": {"type": "monthly_expiry", "name": "Holiday"}}},),
            ({"add": {"foo": {"type": "quarterly_expiry", "name": "Holiday"}}},),
            # # Invalid value.
            ({"add": {"2020-01-01": {"type": "holiday", "foo": "Holiday"}}},),
            (
                {
                    "add": {
                        "2020-01-01": {
                            "type": "holiday",
                            "name": "Holiday",
                            "time": "10:00",
                        }
                    }
                },
            ),
            (
                {
                    "add": {
                        "2020-01-01": {
                            "type": "holiday",
                            "name": "Holiday",
                            "foo": "bar",
                        }
                    }
                },
            ),
            (
                {
                    "add": {
                        "2020-01-01": {
                            "type": "monthly_expiry",
                            "foo": "Monthly Expiry",
                        }
                    }
                },
            ),
            (
                {
                    "add": {
                        "2020-01-01": {
                            "type": "quarterly_expiry",
                            "foo": "Quarterly Expiry",
                        }
                    }
                },
            ),
            # Invalid day type.
            (
                {
                    "add": {
                        "2020-01-01": {
                            "type": "foo",
                            "name": "Special Open",
                            "time": "10:00",
                        }
                    }
                },
            ),
            (
                {
                    "add": {
                        "2020-01-01": {
                            "type": "foo",
                            "name": "Special Close",
                            "time": "10:00",
                        }
                    }
                },
            ),
            # Invalid date.
            (
                {
                    "add": {
                        "foo": {
                            "type": "special_open",
                            "name": "Special Open",
                            "time": "10:00",
                        }
                    }
                },
            ),
            (
                {
                    "add": {
                        "foo": {
                            "type": "special_close",
                            "name": "Special Close",
                            "time": "10:00",
                        }
                    }
                },
            ),
            # Invalid value key.
            (
                {
                    "add": {
                        "2020-01-01": {
                            "type": "special_open",
                            "foo": "Special Open",
                            "time": "10:00",
                        }
                    }
                },
            ),
            (
                {
                    "add": {
                        "2020-01-01": {
                            "type": "special_open",
                            "name": "Special Open",
                            "foo": "10:00",
                        }
                    }
                },
            ),
            (
                {
                    "add": {
                        "2020-01-01": {
                            "type": "special_close",
                            "foo": "Special Close",
                            "time": "10:00",
                        }
                    }
                },
            ),
            (
                {
                    "add": {
                        "2020-01-01": {
                            "type": "special_close",
                            "name": "Special Close",
                            "foo": "10:00",
                        }
                    }
                },
            ),
            # Invalid date.
            ({"remove": ["2020-01-01", "foo"]},),
        ],
    )
    def test_changeset_from_invalid_dict(self, d: dict):
        with pytest.raises(ValidationError):
            ChangeSet.model_validate(d)

    @pytest.mark.parametrize(
        ["date", "props1", "props2"],
        [
            # Same day added twice for different day types.
            (
                "2020-01-01",
                {"type": "holiday", "name": "Holiday"},
                {"type": "special_open", "name": "Special Open", "time": "10:00"},
            ),
            (
                "2020-01-01",
                {"type": "holiday", "name": "Holiday"},
                {"type": "special_close", "name": "Special Close", "time": "10:00"},
            ),
            (
                "2020-01-01",
                {"type": "holiday", "name": "Holiday"},
                {"type": "monthly_expiry", "name": "Monthly Expiry"},
            ),
            (
                "2020-01-01",
                {"type": "holiday", "name": "Holiday"},
                {"type": "quarterly_expiry", "name": "Quarterly Expiry"},
            ),
        ],
    )
    def test_changeset_from_inconsistent_dict(
        self, date: DateLike, props1: DayPropsLike, props2: DayPropsLike
    ):
        # Empty changeset.
        cs = ChangeSet()

        # Add day.
        cs.add_day(date, props1)

        with pytest.raises(ValueError):
            cs.add_day(date, props2)

    @pytest.mark.parametrize(
        ["include_meta"], [(True,), (False,)], ids=["include_meta", "exclude_meta"]
    )
    def test_all_days(self, include_meta: bool):
        cs = ChangeSet(
            add={
                "2020-01-01": {"type": "holiday", "name": "Holiday"},
                "2020-02-01": {
                    "type": "special_open",
                    "name": "Special Open",
                    "time": "10:00",
                },
                "2020-03-01": {
                    "type": "special_close",
                    "name": "Special Close",
                    "time": "16:00",
                },
                "2020-04-01": {"type": "monthly_expiry", "name": "Monthly Expiry"},
                "2020-05-01": {"type": "quarterly_expiry", "name": "Quarterly Expiry"},
            },
            remove=[
                "2020-01-02",
                "2020-02-02",
                "2020-03-02",
                "2020-04-02",
                "2020-05-02",
            ],
            meta={
                "2020-01-03": {"tags": ["foo", "bar"]},
                "2020-02-03": {"tags": ["foo", "bar"]},
                "2020-03-03": {"tags": ["foo", "bar"]},
                "2020-04-03": {"tags": ["foo", "bar"]},
                "2020-05-03": {"tags": ["foo", "bar"]},
                "2020-01-04": {"comment": "This is a comment."},
                "2020-02-04": {"comment": "This is a comment."},
                "2020-03-04": {"comment": "This is a comment."},
                "2020-04-04": {"comment": "This is a comment."},
                "2020-05-04": {"comment": "This is a comment."},
            },
        )
        assert cs.all_days(include_meta=include_meta) == tuple(
            sorted(
                map(
                    TimeStampValidator.validate_python,
                    [
                        "2020-01-01",
                        "2020-01-02",
                        "2020-02-01",
                        "2020-02-02",
                        "2020-03-01",
                        "2020-03-02",
                        "2020-04-01",
                        "2020-04-02",
                        "2020-05-01",
                        "2020-05-02",
                    ]
                    + (
                        [
                            "2020-01-03",
                            "2020-02-03",
                            "2020-03-03",
                            "2020-04-03",
                            "2020-05-03",
                            "2020-01-04",
                            "2020-02-04",
                            "2020-03-04",
                            "2020-04-04",
                            "2020-05-04",
                        ]
                        if include_meta
                        else []
                    ),
                )
            )
        )
