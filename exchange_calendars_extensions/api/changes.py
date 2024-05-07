import datetime as dt
import functools
from collections import OrderedDict
from enum import Enum, unique
from typing import Union, Annotated, Callable

import pandas as pd
from pydantic import BaseModel, Field, RootModel, model_validator, validate_call
from pydantic.functional_validators import BeforeValidator, AfterValidator
from typing_extensions import (
    Literal,
    Any,
    Self,
    Concatenate,
    ParamSpec,
    TypeVar,
)


@unique
class DayType(str, Enum):
    """
    Enum for the different types of holidays and special sessions.

    Assumed to be mutually exclusive, e.g., a special open day cannot be a monthly expiry day as well, although both are
    business days.

    HOLIDAY: A holiday.
    SPECIAL_OPEN: A special session with a special opening time.
    SPECIAL_CLOSE: A special session with a special closing time.
    MONTHLY_EXPIRY: A monthly expiry.
    QUARTERLY_EXPIRY: A quarterly expiry.
    """

    HOLIDAY = "holiday"
    SPECIAL_OPEN = "special_open"
    SPECIAL_CLOSE = "special_close"
    MONTHLY_EXPIRY = "monthly_expiry"
    QUARTERLY_EXPIRY = "quarterly_expiry"


def _to_timestamp(value: Any) -> pd.Timestamp:
    """
    Convert value to Pandas timestamp.

    Parameters
    ----------
    value : Union[pd.Timestamp, str]
        The value to convert.

    Returns
    -------
    pd.Timestamp
        The converted value.

    Raises
    ------
    ValueError
        If the value cannot be converted to pd.Timestamp or converts to pd.NaT.
    """
    # Check if value is a valid timestamp.
    if not isinstance(value, pd.Timestamp):
        try:
            # Convert value to timestamp.
            # noinspection PyTypeChecker
            value = pd.Timestamp(value)
        except ValueError as e:
            # Failed to convert key to timestamp.
            raise ValueError(
                f'Failed to convert "{value}" of type {type(value)} to {pd.Timestamp}.'
            ) from e
        else:
            if value is pd.NaT:
                # Failed to convert key to timestamp.
                raise ValueError(
                    f'Failed to convert "{value}" of type {type(value)} to {pd.Timestamp}.'
                )

    return value


def _to_date(value: pd.Timestamp) -> pd.Timestamp:
    """
    Removes timezone information from the given Timestamp and normalizes to midnight.

    Parameters
    ----------
    value : pd.Timestamp
        The input timestamp from which timezone information is to be removed.

    Returns
    -------
    pd.Timestamp
        The timestamp normalized to midnight with timezone information removed.

    """

    # Remove timezone information and normalize to midnight.
    return value.tz_localize(None).normalize()


# A type alias for pd.Timestamp that allows initialisation from suitably formatted string values.
TimestampLike = Annotated[pd.Timestamp, BeforeValidator(_to_timestamp)]

# A type alias for TimestampLike that normalizes the timestamp to a date-like value.
#
# Date-like means that the timestamp is timezone-naive and normalized to the date boundary, i.e. midnight of the day it
# represents. If the input converts to a valid pd.Timestamp, any timezone information, if present, is discarded. If the
# result is not aligned with a date boundary, it is normalized to midnight of the same day.
DateLike = Annotated[TimestampLike, AfterValidator(_to_date)]


class AbstractDayProps(
    BaseModel, arbitrary_types_allowed=True, validate_assignment=True, extra="forbid"
):
    """
    Abstract base class for special day properties.
    """

    name: str  # The name of the day.


class DayProps(AbstractDayProps):
    """
    Vanilla special day specification.
    """

    type: Literal[
        DayType.HOLIDAY, DayType.MONTHLY_EXPIRY, DayType.QUARTERLY_EXPIRY
    ]  # The type of the special day.

    def __str__(self):
        return f'{{type={self.type.name}, name="{self.name}"}}'


def _to_time(value: Union[dt.time, str]):
    """
    Convert value to time.

    Parameters
    ----------
    value : Union[dt.time, str]
        The value to convert.

    Returns
    -------
    dt.time
        The converted value.

    Raises
    ------
    ValueError
        If the value cannot be converted to dt.time.
    """
    if not isinstance(value, dt.time):
        for f in ("%H:%M", "%H:%M:%S"):
            try:
                # noinspection PyTypeChecker
                value = dt.datetime.strptime(value, f).time()
                break
            except ValueError:
                pass

        if not isinstance(value, dt.time):
            raise ValueError(f"Failed to convert {value} to {dt.time}.")

    return value


# A type alias for dt.time that allows initialisation from suitably formatted string values.
TimeLike = Annotated[dt.time, BeforeValidator(_to_time)]


class DayPropsWithTime(AbstractDayProps):
    """
    Special day specification that requires a (open/close) time.
    """

    type: Literal[
        DayType.SPECIAL_OPEN, DayType.SPECIAL_CLOSE
    ]  # The type of the special day.
    time: TimeLike  # The open/close time of the special day.

    def __str__(self):
        return f'{{type={self.type.name}, name="{self.name}", time={self.time}}}'


# Type alias for valid day properties.
DayPropsLike = Annotated[Union[DayProps, DayPropsWithTime], Field(discriminator="type")]

Tags = Union[list[str], Union[tuple[str], Union[set[str], None]]]


class DayMeta(
    BaseModel, arbitrary_types_allowed=True, validate_assignment=True, extra="forbid"
):
    """
    Metadata for a single date.
    """

    # Collection of tags.
    tags: Tags = []

    # Free-form comment.
    comment: Union[str, None] = None

    @model_validator(mode="after")
    def _canonicalize(self) -> "DayMeta":
        # Sort tags alphabetically and remove duplicates.
        self.__dict__["tags"] = sorted(set(self.tags or []))

        # Strip comment of whitespace and set to None if empty.
        if self.comment is not None:
            self.__dict__["comment"] = self.comment.strip() or None

        return self

    def __len__(self):
        return len(self.tags) + (1 if self.comment is not None else 0)


T_Self = TypeVar("S")
P = ParamSpec("P")


def _with_meta(
    f: Callable[Concatenate[T_Self, DayMeta, P], DayMeta],
) -> Callable[Concatenate[T_Self, DateLike, P], T_Self]:
    @functools.wraps(f)
    @validate_call(config={"arbitrary_types_allowed": True})
    def wrapper(
        self: T_Self, date: DateLike, *args: P.args, **kwargs: P.kwargs
    ) -> T_Self:
        # Retrieve meta for given day.
        meta = self.meta.get(date, DayMeta())

        # Call wrapped function with meta as first positional argument.
        meta = f(self, meta, *args, **kwargs)

        # Update meta for date.
        if not meta:
            self.meta.pop(date, None)
        else:
            self.meta[date] = meta

        # Return self.
        return self

    return wrapper


class ChangeSet(
    BaseModel, arbitrary_types_allowed=True, validate_assignment=True, extra="forbid"
):
    """
    Represents a modification to an existing exchange calendar.

    A changeset consists of a set of dates to add and a set of dates to remove, respectively, for each of the following
    types of days:
    - holidays
    - special open
    - special close
    - monthly expiry
    - quarterly expiry

    A changeset is consistent if and only if the following conditions are satisfied:
    1) For each day type, the corresponding dates to add and dates to remove do not overlap.
    2) For each distinct pair of day types, the dates to add must not overlap

    Condition 1) ensures that the same day is not added and removed at the same time for the same day type. Condition 2)
    ensures that the same day is not added for two different day types.

    Consistency does not require a condition similar to 2) for dates to remove. This is because removing a day from a
    calendar can never make it inconsistent. For example, if a changeset contains the same day as a day to remove for
    two different day types, then applying these changes to a calendar will result in the day being removed from the
    calendar at most once (if it was indeed a holiday or special day in the original calendar) or not at all otherwise.
    Therefore, changesets may specify the same day to be removed for multiple day types, just not for day types that
    also add the same date.

    A changeset is normalized if and only if the following conditions are satisfied:
    1) It is consistent.
    2) When applied to an exchange calendar, the resulting calendar is consistent.

    A changeset that is consistent can still cause an exchange calendar to become inconsistent when applied. This is
    because consistency of a changeset requires the days to be added to be mutually exclusive only across all day types
    within the changeset. However, there may be conflicting holidays or special days already present in a given exchange
    calendar to which a changeset is applied. For example, assume the date 2020-01-01 is a holiday in the original
    calendar. Then, a changeset that adds 2020-01-01 as a special open day will cause the resulting calendar to be
    inconsistent. This is because the same day is now both a holiday and a special open day.

    To resolve this issue, the date 2020-01-01 could be added to the changeset, respectively, for all day types (except
    special opens) as a day to remove. Now, if the changeset is applied to the original calendar, 2020-01-01 will no
    longer be a holiday and therefore no longer conflict with the new special open day. This form of sanitization
    ensures that a consistent changeset can be applied safely to any exchange calendar. Effectively, normalization
    ensures that adding a new day for a given day type becomes an upsert operation, i.e. the day is added if it does not
    already exist in any day type category, and updated/moved to the new day type if it does.
    """

    add: dict[DateLike, DayPropsLike] = Field(default_factory=dict)
    remove: list[DateLike] = Field(default_factory=list)
    meta: dict[DateLike, DayMeta] = Field(default_factory=dict)

    @model_validator(mode="after")
    def _canonicalize(self) -> "ChangeSet":
        # Sort days to add by date.
        add = OrderedDict(sorted(self.add.items(), key=lambda i: i[0]))

        # Sort days to remove by date and remove duplicates.
        remove = sorted(set(self.remove))

        # Sort meta by date. Sort tag values and remove duplicates.
        meta = OrderedDict(
            [(k, v) for k, v in sorted(self.meta.items(), key=lambda i: i[0])]
        )

        self.__dict__["add"] = add
        self.__dict__["remove"] = remove
        self.__dict__["meta"] = meta

        return self

    @validate_call(config={"arbitrary_types_allowed": True})
    def add_day(self, date: DateLike, props: DayPropsLike) -> Self:
        """
        Add a day to the change set.

        Parameters
        ----------
        date : DateLike
            The day to add.
        props : Annotated[Union[DayProps, DayPropsWithTime], Field(discriminator='type')]
            The properties of the day to add.

        Returns
        -------
        ExchangeCalendarChangeSet : self
        """

        # Checks if day is already in the dictionary of days to add.
        if date in self.add:
            # Throw an exception.
            raise ValueError(f"Day {date} already in days to add.")

        # Previous value.
        prev = self.add.get(date, None)

        # Add the day to the dictionary of days to add.
        self.add[date] = props

        # Trigger validation.
        try:
            self.model_validate(self, strict=True)
        except Exception as e:
            # Restore previous state.
            if prev is not None:
                self.add[date] = prev
            else:
                del self.add[date]

            # Let exception bubble up.
            raise e

        return self

    @validate_call(config={"arbitrary_types_allowed": True})
    def remove_day(self, date: DateLike) -> Self:
        """
        Remove a day from the change set.

        Parameters
        ----------
        date : DateLike
            The date to remove.

        Returns
        -------
        ExchangeCalendarChangeSet : self

        Raises
        ------
        ValueError
            If removing the given date would make the changeset inconsistent. This can only be if the date is already in
            the days to remove.
        """
        self.remove.append(date)

        try:
            # Trigger validation.
            self.model_validate(self, strict=True)
        except Exception as e:
            self.remove.remove(date)

            # Let exception bubble up.
            raise e

        return self

    @_with_meta
    @validate_call(config={"arbitrary_types_allowed": True})
    def set_tags(self, meta: DayMeta, tags: Tags) -> DayMeta:
        """
        Set the tags of a given day.

        Parameters
        ----------
        meta : DayMeta
            The metadata for the day.
        tags : Tags
            The tags to set for the day. If None or empty, any tags for the day will be removed.

        Returns
        -------
        ExchangeCalendarChangeSet : self
        """

        # Set the tags.
        meta.tags = tags or []

        return meta

    @_with_meta
    @validate_call(config={"arbitrary_types_allowed": True})
    def set_comment(self, meta: DayMeta, comment: Union[str, None]) -> DayMeta:
        """
        Set the comment for a given day.

        Parameters
        ----------
        meta : DayMeta
            The metadata for the day.
        comment : str
            The comment to set for the day. If None or empty, any comment for the day will be removed.

        Returns
        -------
        ExchangeCalendarChangeSet : self
        """

        # Set the tags.
        meta.comment = comment or None

        return meta

    @_with_meta
    @validate_call(config={"arbitrary_types_allowed": True})
    def set_meta(self, meta: DayMeta, meta0: Union[DayMeta, None]) -> DayMeta:
        """
        Set the metadata for a given day.

        Parameters
        ----------
        meta : DayMeta
            The metadata for the day.
        meta0 : DayMeta
            The metadata to set for the day.

        Returns
        -------
        ExchangeCalendarChangeSet : self
        """

        # Set the tags.

        return meta0

    @validate_call(config={"arbitrary_types_allowed": True})
    def clear_day(self, date: DateLike, include_meta: bool = False) -> Self:
        """
        Clear a day from the change set.

        Parameters
        ----------
        date : DateLike
            The date to clear.
        include_meta : bool
            Whether to also remove any metadata associated with the given date.

        Returns
        -------
        ExchangeCalendarChangeSet : self
        """

        # Avoid re-validation since this change cannot make the changeset inconsistent.
        self.__dict__["add"].pop(date, None)
        self.__dict__["remove"] = [x for x in self.remove if x != date]

        if include_meta:
            self.__dict__["meta"].pop(date, None)

        return self

    def clear(self, include_meta: bool = False) -> Self:
        """
        Clear all changes.

        Parameters
        ----------
        include_meta : bool
            Whether to also clear any metadata.

        Returns
        -------
        ExchangeCalendarChangeSet : self
        """
        self.add.clear()
        self.remove.clear()

        if include_meta:
            self.meta.clear()

        return self

    def __len__(self):
        return len(self.add) + len(self.remove) + len(self.meta)

    def __eq__(self, other):
        if not isinstance(other, ChangeSet):
            return False

        return (
            self.add == other.add
            and self.remove == other.remove
            and self.meta == other.meta
        )

    def all_days(self, include_meta: bool = False) -> tuple[pd.Timestamp, ...]:
        """
        All unique dates contained in the changeset.

        This is the union of the dates to add and the dates to remove, with any duplicates removed.

        Parameters
        ----------
        include_meta : bool
            Whether to also include any days for which metadata has been set.

        Returns
        -------
        Tuple[pd.Timestamp, ...]
            All unique days in the changeset.
        """
        # Take union of dates to add and dates to remove.
        dates = set(self.add.keys()).union(set(self.remove))

        # Add dates associated with tags, maybe.
        if include_meta:
            dates = dates.union(set(self.meta.keys()))

        # Return as sorted tuple.
        return tuple(sorted(dates))


# A type alias for a dictionary of changesets, mapping exchange key to a corresponding change set.
class ChangeSetDict(RootModel):
    root: dict[str, ChangeSet]

    # Delegate all dictionary-typical methods to the root dictionary.
    def __getitem__(self, key):
        return self.root[key]

    def __setitem__(self, key, value):
        self.root[key] = value

    def __delitem__(self, key):
        del self.root[key]

    def __iter__(self):
        return iter(self.root)

    def __len__(self):
        return len(self.root)

    def __contains__(self, key):
        return key in self.root

    def keys(self):
        return self.root.keys()

    def values(self):
        return self.root.values()

    def items(self):
        return self.root.items()

    def get(self, key, default=None):
        return self.root.get(key, default)

    def pop(self, key, default=None):
        return self.root.pop(key, default)
