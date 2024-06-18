from typing import cast
from sqlalchemy import or_
from sqlalchemy.orm import InstrumentedAttribute
from sqlalchemy.sql.elements import BinaryExpression


class MakeQueryFilters:
    """Class to make sqlalchemy filters for query params."""

    @staticmethod
    def __make_integer_filters(filters: dict[InstrumentedAttribute, str]) -> set:
        """
        Private method to make sqlalchemy filters conditions for integer query params.
        :param filters: dict of integer conditions.
        :return: set of SQLAlchemy or conditions.
        """
        int_filters = set()
        for key, value in filters.items():
            if value:
                split_values = value.split(",")

                if len(split_values) > 1:
                    int_filters.add(key.in_([int(value) for value in split_values]))

                if len(split_values) == 1:
                    int_filters.add(cast(BinaryExpression, key == int(value)))

        return int_filters

    @staticmethod
    def __make_string_filters(filters: dict[InstrumentedAttribute, str]) -> set:
        """
        Private method to make sqlalchemy filters conditions for string query params.
        :param filters: dict of string conditions.
        :return: set of SQLAlchemy or conditions.
        """
        str_filters = set()
        for key, value in filters.items():
            if value:
                split_values = value.split(",")

                if len(split_values) > 1:
                    str_filters.add(
                        or_(*[key.ilike(f"{value}") for value in split_values])
                    )

                if len(split_values) == 1:
                    str_filters.add(key.ilike(f"{split_values[0]}"))

        return str_filters

    @staticmethod
    def __clean_null_values(
        filters: dict[InstrumentedAttribute, str] = {},
    ) -> dict[InstrumentedAttribute, str]:
        """
        Private method to make clean filters conditions if they have null values.
        :param filters: dict of conditions.
        :return: dict of conditions.
        """
        return {key: value for key, value in filters.items() if value}

    @staticmethod
    def make_filters(
        integer_filters: dict[InstrumentedAttribute, str] = {},
        string_filters: dict[InstrumentedAttribute, str] = {},
        min_integer_filters: dict[InstrumentedAttribute, str] = {},
        max_integer_filters: dict[InstrumentedAttribute, str] = {},
        min_timestamp_filters: dict[InstrumentedAttribute, str] = {},
        max_timestamp_filters: dict[InstrumentedAttribute, str] = {},
    ) -> set:
        """
        Method to make diferent sqlalchemy filters conditions into a set of conditions.
        :param integer_filters: dict of integer conditions.
        :param string_filters: dict of string conditions.
        :param min_integer_filters: dict of min integer conditions.
        :param max_integer_filters: dict of max integer conditions.
        :param min_timestamp_filters: dict of min timestamp conditions.
        :param max_timestamp_filters: dict of max timestamp conditions.
        :return: set of conditions.
        """
        integer_filters = MakeQueryFilters.__clean_null_values(integer_filters)
        string_filters = MakeQueryFilters.__clean_null_values(string_filters)
        min_integer_filters = MakeQueryFilters.__clean_null_values(min_integer_filters)
        max_integer_filters = MakeQueryFilters.__clean_null_values(max_integer_filters)
        min_timestamp_filters = MakeQueryFilters.__clean_null_values(
            min_timestamp_filters
        )
        max_timestamp_filters = MakeQueryFilters.__clean_null_values(
            max_timestamp_filters
        )

        return {
            *MakeQueryFilters.__make_integer_filters(integer_filters),
            *MakeQueryFilters.__make_string_filters(string_filters),
            *{key >= value for key, value in min_integer_filters.items()},
            *{key <= value for key, value in max_integer_filters.items()},
            *{key >= value for key, value in min_timestamp_filters.items()},
            *{key <= value for key, value in max_timestamp_filters.items()},
        }
