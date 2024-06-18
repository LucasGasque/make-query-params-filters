from unittest.mock import Mock, call, create_autospec
from sqlalchemy.orm import InstrumentedAttribute
from sqlalchemy.sql.expression import BinaryExpression
from sqlalchemy.sql.elements import ColumnElement
from make_query_param_filters import MakeQueryFilters


def test_make_integer_filters_single_value():
    mock_key = Mock(spec=InstrumentedAttribute)
    filters = {mock_key: "1"}

    result = MakeQueryFilters._MakeQueryFilters__make_integer_filters(filters)

    assert len(result) == 1
    assert any(mock_key == int(value) for value in filters.values()) in result


def test_make_integer_filters_multiple_values():
    mock_key = Mock(spec=InstrumentedAttribute)
    filters = {mock_key: "1,2,3"}

    result = MakeQueryFilters._MakeQueryFilters__make_integer_filters(filters)

    assert len(result) == 1
    assert mock_key.in_([1, 2, 3]) in result


def test_make_string_filters_single_value():
    mock_key = Mock(spec=InstrumentedAttribute)
    filters = {mock_key: "test"}

    result = MakeQueryFilters._MakeQueryFilters__make_string_filters(filters)

    assert len(result) == 1
    assert mock_key.ilike(f"{filters[mock_key]}") in result


def test_make_string_filters_multiple_values():
    mock_key = create_autospec(ColumnElement, instance=True)
    mock_value = create_autospec(ColumnElement, instance=True)
    mock_key.ilike.return_value = BinaryExpression(mock_key, mock_value, None)

    filters = {mock_key: "test1,test2"}
    result = MakeQueryFilters._MakeQueryFilters__make_string_filters(filters)

    assert len(result) == 1
    expected_calls = [call("test1"), call("test2")]
    mock_key.ilike.assert_has_calls(expected_calls, any_order=True)


def test_clean_null_values():
    mock_key1 = Mock(spec=InstrumentedAttribute)
    mock_key2 = Mock(spec=InstrumentedAttribute)

    filters = {mock_key1: "value", mock_key2: None}

    result = MakeQueryFilters._MakeQueryFilters__clean_null_values(filters)

    assert result == {mock_key1: "value"}


def test_make_filters():
    mock_int_attr = Mock(spec=InstrumentedAttribute)
    mock_str_key = create_autospec(ColumnElement, instance=True)
    mock_str_value = create_autospec(ColumnElement, instance=True)

    mock_str_key.ilike.return_value = BinaryExpression(
        mock_str_key, mock_str_value, None
    )

    integer_filters = {mock_int_attr: "1,2,3"}
    string_filters = {mock_str_key: "test1,test2"}

    result_filters = MakeQueryFilters.make_filters(integer_filters, string_filters)

    assert len(result_filters) == 2
    assert mock_int_attr.in_([1, 2, 3]) in result_filters

    expected_calls = [call("test1"), call("test2")]
    mock_str_key.ilike.assert_has_calls(expected_calls, any_order=True)
