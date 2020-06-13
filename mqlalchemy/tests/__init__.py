"""
    mqlalchemy.tests.__init__
    ~~~~~~~~~~~~~~~~~~~~~~~~~

    Tests for our new query syntax.

"""
# :copyright: (c) 2020 by Nicholas Repole and contributors.
#             See AUTHORS for more details.
# :license: MIT - See LICENSE for more details.
from __future__ import unicode_literals
import unittest
import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.types import (
    String, Integer, Boolean,
    Date, DateTime, Float, Time)
import mqlalchemy
from mqlalchemy.tests import models
from mqlalchemy import (
    apply_mql_filters, convert_to_alchemy_type, InvalidMqlException)
import datetime


class MQLAlchemyTests(unittest.TestCase):

    """A collection of MQLAlchemy tests."""

    def setUp(self):
        """Configure a db session for the chinook database."""
        connect_string = "sqlite+pysqlite:///" + os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "chinook.sqlite")
        self.db_engine = create_engine(connect_string)
        self.DBSession = sessionmaker(bind=self.db_engine)
        self.db_session = self.DBSession()

    def test_db(self):
        """Make sure our test db is functional."""
        result = self.db_session.query(models.Album).filter(
            models.Album.album_id == 1).all()
        self.assertTrue(len(result) == 1)
        self.assertTrue(result[0].artist_id == 1)

    def test_simple_query(self):
        """Test a very simple mqlalchemy query."""
        query = apply_mql_filters(
            self.db_session,
            models.Album,
            {"album_id": 2}
        )
        result = query.all()
        self.assertTrue(len(result) == 1)
        self.assertTrue(result[0].title == "Balls to the Wall")

    def test_simple_prior_query(self):
        """Test a simple mqlalchemy query using a preformed query."""
        query = self.db_session.query(models.Album).filter(
            models.Album.artist_id == 2)
        query = apply_mql_filters(
            query,
            models.Album,
            {"album_id": 2}
        )
        result = query.all()
        self.assertTrue(len(result) == 1)
        self.assertTrue(result[0].title == "Balls to the Wall")

    def test_no_match(self):
        """Test that a query that should have no match works."""
        query = apply_mql_filters(
            self.db_session,
            models.Playlist,
            {"tracks.track_id": 7,
             "playlist_id": 4}
        )
        result = query.all()
        self.assertTrue(len(result) == 0)

    def test_list_relation(self):
        """Test that a list relation .any query works."""
        query = apply_mql_filters(
            self.db_session,
            models.Playlist,
            {"tracks.track_id": 7}
        )
        result = query.all()
        self.assertTrue(len(result) == 2)
        self.assertTrue(
            (result[0].playlist_id == 1 and result[1].playlist_id == 8) or
            (result[0].playlist_id == 8 and result[1].playlist_id == 1))

    def test_complex_list_relation(self):
        """Test that a multi-level list relation query works."""
        query = apply_mql_filters(
            self.db_session,
            models.Album,
            {"tracks.playlists.playlist_id": 18}
        )
        result = query.all()
        self.assertTrue(len(result) == 1)
        self.assertTrue(result[0].album_id == 48)

    def test_more_complex_list_relation(self):
        """Test that a complex list relation query works."""
        query = apply_mql_filters(
            self.db_session,
            models.Album,
            {"tracks": {
                "$elemMatch": {
                    "playlists.playlist_id": 18
                }
            }}
        )
        result = query.all()
        self.assertTrue(len(result) == 1)
        self.assertTrue(result[0].album_id == 48)

    def test_complex_convert_name(self):
        """Test that converting from camelCase to underscore works."""
        query = apply_mql_filters(
            self.db_session,
            models.Album,
            {"TRACKS.PLAYLISTS.PLAYLIST_ID": 18},
            convert_key_names_func=lambda txt: txt.lower()
        )
        result = query.all()
        self.assertTrue(len(result) == 1)
        self.assertTrue(result[0].album_id == 48)

    def test_explicit_elem_match(self):
        """Test that an explicit elemMatch."""
        query = apply_mql_filters(
            self.db_session,
            models.Playlist,
            {"tracks": {
                "$elemMatch": {
                    "track_id": 7
                }
            }}
        )
        result = query.all()
        self.assertTrue(len(result) == 2)
        self.assertTrue(
            (result[0].playlist_id == 1 and result[1].playlist_id == 8) or
            (result[0].playlist_id == 8 and result[1].playlist_id == 1))

    def test_list_relation_eq_fail(self):
        """Make sure we can't check a relation for equality."""
        self.assertRaises(
            InvalidMqlException,
            apply_mql_filters,
            self.db_session,
            models.Playlist,
            {"tracks": 7}
        )

    def test_list_relation_neq_fail(self):
        """Make sure we can't check a relation for inequality."""
        self.assertRaises(
            InvalidMqlException,
            apply_mql_filters,
            self.db_session,
            models.Playlist,
            {"tracks": {"$ne": 7}}
        )

    def test_non_list_relation(self):
        """Test that a non-list relation .has query works."""
        query = apply_mql_filters(
            self.db_session,
            models.Album,
            {"artist.artist_id": 275}
        )
        result = query.all()
        self.assertTrue(len(result) == 1)
        self.assertTrue(result[0].album_id == 347)

    def test_implicit_and(self):
        """Test that an implicit and query works."""
        query = apply_mql_filters(
            self.db_session,
            models.Playlist,
            {"tracks.track_id": 7,
             "playlist_id": 1}
        )
        result = query.all()
        self.assertTrue(len(result) == 1)
        self.assertTrue(result[0].playlist_id == 1)

    def test_explicit_and(self):
        """Test that the $and operator works."""
        query = apply_mql_filters(
            self.db_session,
            models.Playlist,
            {"$and": [
                {"tracks.track_id": 7},
                {"playlist_id": 1}
            ]}
        )
        result = query.all()
        self.assertTrue(len(result) == 1)
        self.assertTrue(result[0].playlist_id == 1)

    def test_or(self):
        """Test that the $or operator works."""
        query = apply_mql_filters(
            self.db_session,
            models.Playlist,
            {"$or": [
                {"tracks.track_id": 999999},
                {"playlist_id": 1}
            ]}
        )
        result = query.all()
        self.assertTrue(len(result) == 1)
        self.assertTrue(result[0].playlist_id == 1)

    def test_negation(self):
        """Test that the $not operator works."""
        query = apply_mql_filters(
            self.db_session,
            models.Playlist,
            {"tracks.track_id": 7,
             "$not": {
                 "playlist_id": 1
             }}
        )
        result = query.all()
        self.assertTrue(len(result) == 1)
        self.assertTrue(result[0].playlist_id == 8)

    def test_nor(self):
        """Test that the $nor operator works."""
        query = apply_mql_filters(
            self.db_session,
            models.Playlist,
            {"tracks.track_id": 7,
             "$nor": [
                 {"playlist_id": 1},
                 {"playlist_id": 999}
             ]}
        )
        result = query.all()
        self.assertTrue(len(result) == 1)
        self.assertTrue(result[0].playlist_id == 8)

    def test_neq(self):
        """Test that the $ne operator works."""
        query = apply_mql_filters(
            self.db_session,
            models.Playlist,
            {"tracks.track_id": 7,
             "playlist_id": {
                 "$ne": 1
             }}
        )
        result = query.all()
        self.assertTrue(len(result) == 1)
        self.assertTrue(result[0].playlist_id == 8)

    def test_lt(self):
        """Test that the $lt operator works."""
        query = apply_mql_filters(
            self.db_session,
            models.Playlist,
            {"playlist_id": {
                "$lt": 2
            }}
        )
        result = query.all()
        self.assertTrue(len(result) == 1)
        self.assertTrue(result[0].playlist_id == 1)

    def test_lte(self):
        """Test that the $lte operator works."""
        query = apply_mql_filters(
            self.db_session,
            models.Playlist,
            {"playlist_id": {
                "$lte": 1
            }}
        )
        result = query.all()
        self.assertTrue(len(result) == 1)
        self.assertTrue(result[0].playlist_id == 1)

    def test_eq(self):
        """Test that the new $eq operator works."""
        query = apply_mql_filters(
            self.db_session,
            models.Playlist,
            {"playlist_id": {
                "$eq": 1
            }}
        )
        result = query.all()
        self.assertTrue(len(result) == 1)
        self.assertTrue(result[0].playlist_id == 1)

    def test_gte(self):
        """Test that the $gte operator works."""
        query = apply_mql_filters(
            self.db_session,
            models.Playlist,
            {"playlist_id": {
                "$gte": 18
            }}
        )
        result = query.all()
        self.assertTrue(len(result) == 1)
        self.assertTrue(result[0].playlist_id == 18)

    def test_gt(self):
        """Test that the $gt operator works."""
        query = apply_mql_filters(
            self.db_session,
            models.Playlist,
            {"playlist_id": {
                "$gt": 17
            }}
        )
        result = query.all()
        self.assertTrue(len(result) == 1)
        self.assertTrue(result[0].playlist_id == 18)

    def test_mod(self):
        """Test that the $mod operator works."""
        query = apply_mql_filters(
            self.db_session,
            models.Playlist,
            {"playlist_id": {
                "$mod": [18, 0]
            }}
        )
        result = query.all()
        self.assertTrue(len(result) == 1)
        self.assertTrue(result[0].playlist_id == 18)

    def test_mod_str_fail(self):
        """Test passing string values to $mod op fails."""
        self.assertRaises(
            InvalidMqlException,
            apply_mql_filters,
            self.db_session,
            models.Playlist,
            {"playlist_id": {
                "$mod": ["test", "hey"]
            }}
        )

    def test_mod_decimal_divisor_fails(self):
        """Test passing a decimal divisor to $mod op fails."""
        self.assertRaises(
            InvalidMqlException,
            apply_mql_filters,
            self.db_session,
            models.Playlist,
            {"playlist_id": {
                "$mod": [2.2, 4]
            }}
        )

    def test_mod_decimal_remainder_fails(self):
        """Test passing a decimal remainder to $mod op fails."""
        self.assertRaises(
            InvalidMqlException,
            apply_mql_filters,
            self.db_session,
            models.Playlist,
            {"playlist_id": {
                "$mod": [2, 4.4]
            }}
        )

    def test_mod_non_list(self):
        """Test passing a non list to $mod op rails."""
        self.assertRaises(
            InvalidMqlException,
            apply_mql_filters,
            self.db_session,
            models.Playlist,
            {"playlist_id": {
                "$mod": 5
            }}
        )

    def test_mod_non_int_field(self):
        """Test trying to $mod a non int field fails."""
        self.assertRaises(
            InvalidMqlException,
            apply_mql_filters,
            self.db_session,
            models.Playlist,
            {"name": {
                "$mod": 5
            }}
        )

    def test_in(self):
        """Test that the $in operator works."""
        query = apply_mql_filters(
            self.db_session,
            models.Playlist,
            {"playlist_id": {
                "$in": [1, 2]
            }}
        )
        result = query.all()
        self.assertTrue(
            len(result) == 2)
        self.assertTrue(
            (result[0].playlist_id == 1 and result[1].playlist_id == 2) or
            (result[0].playlist_id == 2 and result[1].playlist_id == 1))

    def test_in_nested(self):
        """Test that the $in operator works on nested objects."""
        query = apply_mql_filters(
            self.db_session,
            models.Playlist,
            {"tracks.track_id": {
                "$in": [7, 9999]
            }}
        )
        result = query.all()
        self.assertTrue(
            len(result) == 2 and
            ((result[0].playlist_id == 1 and result[1].playlist_id == 8) or
             (result[0].playlist_id == 8 and result[1].playlist_id == 1)))

    def test_in_non_list_fails(self):
        """Test that the $in op fails when not supplied with a list."""
        self.assertRaises(
            InvalidMqlException,
            apply_mql_filters,
            self.db_session,
            models.Playlist,
            {"playlist_id": {
                "$in": 1
            }}
        )

    def test_nin(self):
        """Test that the $nin operator works."""
        query = apply_mql_filters(
            self.db_session,
            models.Playlist,
            {"playlist_id": {
                "$nin": [
                    2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18]
            }}
        )
        result = query.all()
        self.assertTrue(len(result) == 1 and
                        result[0].playlist_id == 1)

    def test_nin_non_list_fails(self):
        """Test that the $nin op fails when not supplied with a list."""
        self.assertRaises(
            InvalidMqlException,
            apply_mql_filters,
            self.db_session,
            models.Playlist,
            {"playlist_id": {
                "$nin": 1
            }}
        )

    def test_like(self):
        """Test that the new $like operator works."""
        query = apply_mql_filters(
            self.db_session,
            models.Employee,
            {"first_name": {
                "$like": "tev"
            }}
        )
        result = query.all()
        self.assertTrue(len(result) == 1)
        self.assertTrue(result[0].first_name == "Steve")

    def test_elemmatch_fail(self):
        """Test that the $elemMatch operator properly fails."""
        self.assertRaises(
            InvalidMqlException,
            apply_mql_filters,
            self.db_session,
            models.Employee,
            {"first_name": {
                "$elemMatch": {"test": "test"}
            }}
        )

    def test_nested_attr_query_fail(self):
        """Test that a nested attribute query fails."""
        self.assertRaises(
            InvalidMqlException,
            apply_mql_filters,
            self.db_session,
            models.Track,
            {"track_id": {
                "info": 5
            }}
        )

    def test_bad_operator_fail(self):
        """Test that a invalid operator fails."""
        self.assertRaises(
            InvalidMqlException,
            apply_mql_filters,
            self.db_session,
            models.Track,
            {"track_id": {
                "$bad": 5
            }},
            ["track_id"]
        )

    def test_empty_dict_fail(self):
        """Test that a nested attribute query fails."""
        self.assertRaises(
            InvalidMqlException,
            apply_mql_filters,
            self.db_session,
            models.Playlist,
            {"tracks": {}}
        )

    def test_whitelist(self):
        """Test that whitelisting works as expected."""
        self.assertRaises(
            InvalidMqlException,
            apply_mql_filters,
            self.db_session,
            models.Playlist,
            {"tracks.track_id": 7},
            []
        )
        self.assertFalse(
            mqlalchemy._is_whitelisted(
                models.Album,
                "bad_attr_name",
                ["bad_attr_name"]
            )
        )
        query = apply_mql_filters(
            self.db_session,
            models.Playlist,
            {"tracks.track_id": 7},
            ["tracks.track_id"]
        )
        result = query.all()
        self.assertTrue(
            len(result) == 2)
        self.assertTrue(
            (result[0].playlist_id == 1 and result[1].playlist_id == 8) or
            (result[0].playlist_id == 8 and result[1].playlist_id == 1))

    def test_custom_whitelist_func(self):
        """Test that providing a whitelist function works."""
        def whitelist(attr_name):
            if attr_name == "tracks.track_id":
                return True
            return False
        query = apply_mql_filters(
            self.db_session,
            models.Playlist,
            {"tracks.track_id": 7},
            whitelist
        )
        result = query.all()
        self.assertTrue(
            len(result) == 2)
        self.assertTrue(
            (result[0].playlist_id == 1 and result[1].playlist_id == 8) or
            (result[0].playlist_id == 8 and result[1].playlist_id == 1))
        self.assertRaises(
            InvalidMqlException,
            apply_mql_filters,
            self.db_session,
            models.Playlist,
            {"tracks.name": "Test"},
            whitelist
        )

    def test_convert_to_int(self):
        """Test that we can convert a string to integer."""
        self.assertTrue(convert_to_alchemy_type("1", Integer) == 1)
        self.assertTrue(convert_to_alchemy_type(1, Integer) == 1)

    def test_convert_to_float(self):
        """Test that we can convert a string to a float."""
        self.assertTrue(convert_to_alchemy_type("1.1", Float) == 1.1)
        self.assertTrue(convert_to_alchemy_type(1.1, Float) == 1.1)

    def test_convert_to_bool(self):
        """Test that we can convert a value to a boolean."""
        self.assertFalse(convert_to_alchemy_type("0", Boolean))
        self.assertFalse(convert_to_alchemy_type("FaLSE", Boolean))
        self.assertFalse(convert_to_alchemy_type(False, Boolean))
        self.assertTrue(convert_to_alchemy_type("1", Boolean))
        self.assertTrue(convert_to_alchemy_type("True", Boolean))

    def test_convert_to_datetime(self):
        """Test that we can convert a value to a datetime."""
        self.assertTrue(
            convert_to_alchemy_type(
                "2015-03-11 01:45:14", DateTime) ==
            datetime.datetime(2015, 3, 11, 1, 45, 14))
        self.assertTrue(
            convert_to_alchemy_type(
                datetime.datetime(2015, 3, 11, 1, 45, 14), DateTime) ==
            datetime.datetime(2015, 3, 11, 1, 45, 14))

    def test_convert_to_date(self):
        """Test that we can convert a value to a date."""
        self.assertTrue(
            convert_to_alchemy_type(
                "2015-03-11", Date) == datetime.date(2015, 3, 11))
        self.assertTrue(
            convert_to_alchemy_type(
                datetime.date(2015, 3, 1), Date) == datetime.date(2015, 3, 1))

    def test_convert_to_time(self):
        """Test that we can convert a value to a time."""
        self.assertTrue(
            convert_to_alchemy_type(
                "01:45:14", Time) == datetime.time(1, 45, 14))
        self.assertTrue(
            convert_to_alchemy_type(
                datetime.time(1, 45, 14), Time) == datetime.time(1, 45, 14))

    def test_convert_to_string(self):
        """Test that we can convert a string to integer."""
        self.assertTrue(convert_to_alchemy_type(1, String) == "1")
        self.assertTrue(convert_to_alchemy_type("Hello", String) == "Hello")

    def test_convert_to_null(self):
        """Test that convert_to_alchemy_type properly returns None."""
        self.assertTrue(convert_to_alchemy_type("null", String) is None)
        self.assertTrue(convert_to_alchemy_type(None, String) is None)

    def test_convert_fail(self):
        """Test that convert_to_alchemy_type properly fails."""
        self.assertRaises(
            TypeError,
            convert_to_alchemy_type,
            "blah",
            None)

    def test_get_attr_class_attributes(self):
        """Test that _get_class_attributes works."""
        class_attrs = mqlalchemy._get_class_attributes(
            models.Album,
            "tracks.0.track_id")
        self.assertTrue(len(class_attrs) == 4)

    def test_stack_size_limit(self):
        """Make sure that limiting the stack size works as expected."""
        query = apply_mql_filters(
            self.db_session,
            models.Album,
            filters={
                "album_id": 1,
                "title": "For Those About To Rock We Salute You"},
            stack_size_limit=10
        )
        result = query.all()
        self.assertTrue(len(result) == 1)
        self.assertTrue(result[0].album_id == 1)

    def test_stack_size_limit_fail(self):
        """Make sure that limiting the stack size fails as expected."""
        self.assertRaises(
            InvalidMqlException,
            apply_mql_filters,
            self.db_session,
            models.Album,
            filters={
                "album_id": 1,
                "title": "For Those About To Rock We Salute You"},
            stack_size_limit=1
        )

    def test_type_conversion_fail(self):
        """Make sure we can't check a relation for equality."""
        self.assertRaises(
            InvalidMqlException,
            apply_mql_filters,
            self.db_session,
            models.Playlist,
            {"playlist_id": "test"}
        )

    def test_self_referential_relation(self):
        """Test relationship chain leading to the same model."""
        query = apply_mql_filters(
            self.db_session,
            models.Album,
            {"tracks.album.album_id": 18}
        )
        result = query.all()
        self.assertTrue(len(result) == 1)
        self.assertTrue(result[0].album_id == 18)

    def test_required_filters(self):
        """Test required filters are applied properly."""
        required_filter_log = {}

        def required_filters(key):
            """Return required filters for a relation based on key name.

            :param str key: Dot separated data key, relative to the
                root model.
            :return: Any required filters to be applied to the child
                relationship.

            """
            # Do some logging of how many times each key is hit
            required_filter_log[key] = (required_filter_log.get(key) or 0) + 1
            if key == "tracks":
                return models.Track.album.has(models.Album.album_id != 18)

        # Search playlist for a track that is explicitly excluded
        # via required_filters
        query = apply_mql_filters(
            self.db_session,
            models.Playlist,
            {"tracks.track_id": 166},
            required_filters=required_filters
        )
        self.assertTrue(required_filter_log.get("tracks") == 1)
        result = query.all()
        self.assertTrue(len(result) == 0)

    def test_required_filters_dict(self):
        """Test dict required filters are applied properly."""
        required_filters = {
            "tracks": (
                models.Track.album.has(models.Album.album_id != 18))}

        # Search playlist for a track that is explicitly excluded
        # via required_filters
        query = apply_mql_filters(
            self.db_session,
            models.Playlist,
            {"tracks.track_id": 166},
            required_filters=required_filters
        )
        result = query.all()
        self.assertTrue(len(result) == 0)


if __name__ == '__main__':    # pragma no cover
    unittest.main()
