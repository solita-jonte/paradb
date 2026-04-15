"""Tests for the query engine — matching documents against MongoDB-like filters."""

from shard.query_engine import match


class TestExactMatchSingleField:
    def test_matching_value(self):
        # given a document with name "Alice"
        doc = {"_id": "abc", "name": "Alice"}

        # when we query for name "Alice"
        # then it matches
        assert match(doc, {"name": "Alice"}) is True

    def test_non_matching_value(self):
        # given a document with name "Alice"
        doc = {"_id": "abc", "name": "Alice"}

        # when we query for name "Bob"
        # then it does not match
        assert match(doc, {"name": "Bob"}) is False


class TestExactMatchMultipleFields:
    def test_all_fields_match(self):
        # given a document with name and age
        doc = {"_id": "abc", "name": "Alice", "age": 30}

        # when we query for both fields matching
        # then it matches
        assert match(doc, {"name": "Alice", "age": 30}) is True

    def test_one_field_does_not_match(self):
        # given a document with name and age
        doc = {"_id": "abc", "name": "Alice", "age": 30}

        # when one field doesn't match
        # then it does not match
        assert match(doc, {"name": "Alice", "age": 25}) is False


class TestComparisonGt:
    def test_value_greater_than(self):
        # given a document with age 30
        doc = {"_id": "abc", "age": 30}

        # when querying $gt 25
        # then it matches
        assert match(doc, {"age": {"$gt": 25}}) is True

    def test_value_equal_not_greater(self):
        # given a document with age 30
        doc = {"_id": "abc", "age": 30}

        # when querying $gt 30
        # then it does not match (equal is not greater)
        assert match(doc, {"age": {"$gt": 30}}) is False

    def test_value_less_than(self):
        # given a document with age 30
        doc = {"_id": "abc", "age": 30}

        # when querying $gt 35
        # then it does not match
        assert match(doc, {"age": {"$gt": 35}}) is False


class TestComparisonLt:
    def test_value_less_than(self):
        # given a document with age 30
        doc = {"_id": "abc", "age": 30}

        # when querying $lt 35
        # then it matches
        assert match(doc, {"age": {"$lt": 35}}) is True

    def test_value_equal_not_less(self):
        # given a document with age 30
        doc = {"_id": "abc", "age": 30}

        # when querying $lt 30
        # then it does not match
        assert match(doc, {"age": {"$lt": 30}}) is False


class TestCombinedGtLtRange:
    def test_value_in_range(self):
        # given a document with age 30
        doc = {"_id": "abc", "age": 30}

        # when querying $gt 25 and $lt 35
        # then it matches
        assert match(doc, {"age": {"$gt": 25, "$lt": 35}}) is True

    def test_value_outside_range(self):
        # given a document with age 30
        doc = {"_id": "abc", "age": 30}

        # when querying $gt 31 and $lt 35
        # then it does not match
        assert match(doc, {"age": {"$gt": 31, "$lt": 35}}) is False


class TestExistsTrue:
    def test_field_exists(self):
        # given a document with name field
        doc = {"_id": "abc", "name": "Alice"}

        # when querying $exists true on existing field
        # then it matches
        assert match(doc, {"name": {"$exists": True}}) is True

    def test_field_does_not_exist(self):
        # given a document with name field
        doc = {"_id": "abc", "name": "Alice"}

        # when querying $exists true on missing field
        # then it does not match
        assert match(doc, {"missing_field": {"$exists": True}}) is False


class TestExistsFalse:
    def test_field_exists_but_query_false(self):
        # given a document with name field
        doc = {"_id": "abc", "name": "Alice"}

        # when querying $exists false on existing field
        # then it does not match
        assert match(doc, {"name": {"$exists": False}}) is False

    def test_field_does_not_exist_and_query_false(self):
        # given a document with name field
        doc = {"_id": "abc", "name": "Alice"}

        # when querying $exists false on missing field
        # then it matches
        assert match(doc, {"missing_field": {"$exists": False}}) is True


class TestNestedFieldDotNotation:
    def test_nested_field_match(self):
        # given a document with nested address
        doc = {"_id": "abc", "address": {"city": "Stockholm", "zip": "111"}}

        # when querying with dot notation
        # then it matches
        assert match(doc, {"address.city": "Stockholm"}) is True

    def test_nested_field_no_match(self):
        # given a document with nested address
        doc = {"_id": "abc", "address": {"city": "Stockholm", "zip": "111"}}

        # when querying non-matching value
        # then it does not match
        assert match(doc, {"address.city": "Göteborg"}) is False


class TestDeeplyNestedDotNotation:
    def test_deep_nested_match(self):
        # given a deeply nested document
        doc = {"_id": "abc", "a": {"b": {"c": 42}}}

        # when querying deep dot notation
        # then it matches
        assert match(doc, {"a.b.c": 42}) is True


class TestDotNotationMissingIntermediate:
    def test_missing_intermediate_no_error(self):
        # given a document with no nested structure
        doc = {"_id": "abc", "a": 1}

        # when querying a dot path into non-existent nesting
        # then it does not match (and does not raise an error)
        assert match(doc, {"a.b.c": 42}) is False


class TestEmptyQueryMatchesAll:
    def test_empty_query(self):
        # given any document
        doc = {"_id": "abc", "name": "Alice"}

        # when querying with empty dict
        # then it matches
        assert match(doc, {}) is True


class TestQueryById:
    def test_id_filter(self):
        # given a document
        doc = {"_id": "specific-uuid", "name": "Alice"}

        # when querying by _id
        # then it matches the correct document
        assert match(doc, {"_id": "specific-uuid"}) is True
        assert match(doc, {"_id": "other-uuid"}) is False
