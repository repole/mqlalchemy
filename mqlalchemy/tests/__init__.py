## -*- coding: utf-8 -*-\
"""
    mqlalchemy.tests.__init__
    ~~~~~~~~~~~~~~~~~~~~~~~~~

    Tests for our new query syntax.

    :copyright: (c) 2015 by Nicholas Repole and contributors.
                See AUTHORS for more details.
    :license: BSD - See LICENSE for more details.
"""
from __future__ import unicode_literals
import unittest
import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.types import \
    String, Integer, Boolean, \
    Date, DateTime, Float, Time
import mqlalchemy
from mqlalchemy.tests import models
from mqlalchemy import apply_mql_filters, convert_to_alchemy_type, \
    InvalidMQLException
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
        self.assertTrue(len(result) == 1 and result[0].artist_id == 1)

    def test_simple_query(self):
        """Test a very simple mqlalchemy query."""
        query = apply_mql_filters(
            self.db_session,
            models.Album,
            {"album_id": 2}
        )
        result = query.all()
        self.assertTrue(
            len(result) == 1 and
            result[0].title == "Balls to the Wall")

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
        self.assertTrue(
            len(result) == 1 and
            result[0].title == "Balls to the Wall")

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
        self.assertTrue(
            len(result) == 2 and
            ((result[0].playlist_id == 1 and result[1].playlist_id == 8) or
             (result[0].playlist_id == 8 and result[1].playlist_id == 1)))

    def test_complex_list_relation(self):
        """Test that a multi-level list relation query works."""
        query = apply_mql_filters(
            self.db_session,
            models.Album,
            {"tracks.playlists.playlist_id": 18}
        )
        result = query.all()
        self.assertTrue(
            len(result) == 1 and
            result[0].album_id == 48)

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
        self.assertTrue(
            len(result) == 1 and
            result[0].album_id == 48)

    def test_explicit_elemmatch(self):
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
        self.assertTrue(
            len(result) == 2 and
            ((result[0].playlist_id == 1 and result[1].playlist_id == 8) or
             (result[0].playlist_id == 8 and result[1].playlist_id == 1)))

    def test_list_relation_fail(self):
        """Make sure we can't check a relation for equality."""
        self.assertRaises(
            InvalidMQLException,
            apply_mql_filters,
            self.db_session,
            models.Playlist,
            {"tracks": 7}
        )
        self.assertRaises(
            InvalidMQLException,
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
        self.assertTrue(
            len(result) == 1 and result[0].album_id == 347)

    def test_implicit_and(self):
        """Test that an implicit and query works."""
        query = apply_mql_filters(
            self.db_session,
            models.Playlist,
            {"tracks.track_id": 7,
             "playlist_id": 1}
        )
        result = query.all()
        self.assertTrue(
            len(result) == 1 and
            result[0].playlist_id == 1)

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
        self.assertTrue(
            len(result) == 1 and
            result[0].playlist_id == 1)

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
        self.assertTrue(
            len(result) == 1 and
            result[0].playlist_id == 1)

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
        self.assertTrue(len(result) == 1 and
                        result[0].playlist_id == 8)

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
        self.assertTrue(len(result) == 1 and
                        result[0].playlist_id == 8)

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
        self.assertTrue(len(result) == 1 and
                        result[0].playlist_id == 8)

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
        self.assertTrue(len(result) == 1 and
                        result[0].playlist_id == 1)

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
        self.assertTrue(len(result) == 1 and
                        result[0].playlist_id == 1)

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
        self.assertTrue(len(result) == 1 and
                        result[0].playlist_id == 1)

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
        self.assertTrue(len(result) == 1 and
                        result[0].playlist_id == 18)

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
        self.assertTrue(len(result) == 1 and
                        result[0].playlist_id == 18)

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
        self.assertTrue(len(result) == 1 and
                        result[0].playlist_id == 18)
        self.assertRaises(
            InvalidMQLException,
            apply_mql_filters,
            self.db_session,
            models.Playlist,
            {"playlist_id": {
                "$mod": ["test", "hey"]
            }}
        )
        self.assertRaises(
            InvalidMQLException,
            apply_mql_filters,
            self.db_session,
            models.Playlist,
            {"playlist_id": {
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
            len(result) == 2 and
            ((result[0].playlist_id == 1 and result[1].playlist_id == 2) or
             (result[0].playlist_id == 2 and result[1].playlist_id == 1)))
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
        self.assertRaises(
            InvalidMQLException,
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
        self.assertRaises(
            InvalidMQLException,
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
        self.assertTrue(len(result) == 1 and
                        result[0].first_name == "Steve")

    def test_elemmatch_fail(self):
        """Test that the $elemMatch operator properly fails."""
        self.assertRaises(
            InvalidMQLException,
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
            InvalidMQLException,
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
            InvalidMQLException,
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
            InvalidMQLException,
            apply_mql_filters,
            self.db_session,
            models.Playlist,
            {"tracks": {}}
        )

    def test_whitelist(self):
        """Test that whitelisting works as expected."""
        self.assertRaises(
            InvalidMQLException,
            apply_mql_filters,
            self.db_session,
            models.Playlist,
            {"tracks.track_id": 7},
            []
        )
        self.assertFalse(
            mqlalchemy._is_whitelisted(
                models.Album,
                "Album.bad_attr_name",
                ["Album.bad_attr_name"]
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
            len(result) == 2 and
            ((result[0].playlist_id == 1 and result[1].playlist_id == 8) or
             (result[0].playlist_id == 8 and result[1].playlist_id == 1)))

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
            "Album.tracks.0.track_id")
        self.assertTrue(len(class_attrs) == 4)

    def test_stack_size_limit(self):
        """Make sure that limiting the stack size works as expected."""
        query = apply_mql_filters(
            self.db_session,
            models.Album,
            {"album_id": 1,
             "title": "For Those About To Rock We Salute You"},
            stack_size_limit=10
        )
        result = query.all()
        self.assertTrue(len(result) == 1 and
                        result[0].album_id == 1)
        self.assertRaises(
            InvalidMQLException,
            apply_mql_filters,
            self.db_session,
            models.Album,
            {"album_id": 1,
             "title": "For Those About To Rock We Salute You"},
            [],
            1
        )

if __name__ == '__main__':    # pragma no cover
    unittest.main()
