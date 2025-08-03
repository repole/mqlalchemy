"""
    mqlalchemy.tests.__init__
    ~~~~~~~~~~~~~~~~~~~~~~~~~

    Tests for our new query syntax.

"""
# :copyright: (c) 2016-2025 by Nicholas Repole and contributors.
#             See AUTHORS for more details.
# :license: MIT - See LICENSE for more details.
from __future__ import unicode_literals
import unittest
import os
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker, configure_mappers
from sqlalchemy.types import (
    String, Integer, Boolean,
    Date, DateTime, Float, Time)
import mqlalchemy
from tests.models import (
    Album, Artist, Customer, Employee, Genre, Invoice, InvoiceLine,
    MediaType, Playlist, Track)
from mqlalchemy import (
    apply_mql_filters, convert_to_alchemy_type, InvalidMqlException)
import datetime

# Makes sure backref relationship attrs are attached to models
# e.g. Album.tracks doesn't work without either this or accessing
# Track.album first.
configure_mappers()


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
        stmt = select(Album).where(Album.album_id == 1)
        result = self.db_session.execute(stmt).scalars().all()
        self.assertTrue(len(result) == 1)
        self.assertTrue(result[0].artist_id == 1)

    def test_simple_query(self):
        """Test a very simple mqlalchemy query."""
        stmt = apply_mql_filters(
            model_class=Album,
            filters={"album_id": 2}
        )
        result = self.db_session.execute(stmt).scalars().all()
        self.assertTrue(len(result) == 1)
        self.assertTrue(result[0].title == "Balls to the Wall")

    def test_simple_prior_query(self):
        """Test a simple mqlalchemy query using a preformed query."""
        stmt = select(Album).where(Album.artist_id == 2)
        stmt = apply_mql_filters(
            model_class=Album,
            query=stmt,
            filters={"album_id": 2}
        )
        result = self.db_session.execute(stmt).scalars().all()
        self.assertTrue(len(result) == 1)
        self.assertTrue(result[0].title == "Balls to the Wall")

    def test_no_match(self):
        """Test that a query that should have no match works."""
        stmt = apply_mql_filters(
            model_class=Playlist,
            filters={"tracks.track_id": 7,
                     "playlist_id": 4}
        )
        result = self.db_session.execute(stmt).scalars().all()
        self.assertTrue(len(result) == 0)

    def test_list_relation(self):
        """Test that a list relation .any query works."""
        stmt = apply_mql_filters(
            model_class=Playlist,
            filters={"tracks.track_id": 7}
        )
        result = self.db_session.execute(stmt).scalars().all()
        self.assertTrue(len(result) == 2)
        self.assertTrue(
            (result[0].playlist_id == 1 and result[1].playlist_id == 8) or
            (result[0].playlist_id == 8 and result[1].playlist_id == 1))

    def test_complex_list_relation(self):
        """Test that a multi-level list relation query works."""
        stmt = apply_mql_filters(
            model_class=Album,
            filters={"tracks.playlists.playlist_id": 18}
        )
        result = self.db_session.execute(stmt).scalars().all()
        self.assertTrue(len(result) == 1)
        self.assertTrue(result[0].album_id == 48)

    def test_more_complex_list_relation(self):
        """Test that a complex list relation query works."""
        stmt = apply_mql_filters(
            model_class=Album,
            filters={
                "tracks": {
                    "$elemMatch": {
                        "playlists.playlist_id": 18
                    }
                }
            }
        )
        result = self.db_session.execute(stmt).scalars().all()
        self.assertTrue(len(result) == 1)
        self.assertTrue(result[0].album_id == 48)

    def test_complex_convert_name(self):
        """Test that converting from camelCase to underscore works."""
        stmt = apply_mql_filters(
            model_class=Album,
            filters={"TRACKS.PLAYLISTS.PLAYLIST_ID": 18},
            convert_key_names_func=lambda txt: txt.lower()
        )
        result = self.db_session.execute(stmt).scalars().all()
        self.assertTrue(len(result) == 1)
        self.assertTrue(result[0].album_id == 48)

    def test_explicit_elem_match(self):
        """Test that an explicit elemMatch."""
        stmt = apply_mql_filters(
            model_class=Playlist,
            filters={
                "tracks": {
                    "$elemMatch": {
                        "track_id": 7
                    }
                }
            }
        )
        result = self.db_session.execute(stmt).scalars().all()
        self.assertTrue(len(result) == 2)
        self.assertTrue(
            (result[0].playlist_id == 1 and result[1].playlist_id == 8) or
            (result[0].playlist_id == 8 and result[1].playlist_id == 1))

    def test_implicit_elem_match(self):
        """Test that an implicit elemMatch works."""
        stmt = apply_mql_filters(
            model_class=Playlist,
            filters={"tracks": {"track_id": 7}}
        )
        result = self.db_session.execute(stmt).scalars().all()
        self.assertTrue(len(result) == 2)
        self.assertTrue(
            (result[0].playlist_id == 1 and result[1].playlist_id == 8) or
            (result[0].playlist_id == 8 and result[1].playlist_id == 1))

    def test_list_relation_eq_fail(self):
        """Make sure we can't check a relation for equality."""
        self.assertRaises(
            InvalidMqlException,
            apply_mql_filters,
            model_class=Playlist,
            filters={"tracks": 7}
        )

    def test_list_relation_neq_fail(self):
        """Make sure we can't check a relation for inequality."""
        self.assertRaises(
            InvalidMqlException,
            apply_mql_filters,
            model_class=Playlist,
            filters={"tracks": {"$ne": 7}}
        )

    def test_non_list_relation(self):
        """Test that a non-list relation .has query works."""
        stmt = apply_mql_filters(
            model_class=Album,
            filters={"artist.artist_id": 275}
        )
        result = self.db_session.execute(stmt).scalars().all()
        self.assertTrue(len(result) == 1)
        self.assertTrue(result[0].album_id == 347)

    def test_attr_exists(self):
        """Test $exists on a simple attr."""
        stmt = apply_mql_filters(
            model_class=Customer,
            filters={"company": {"$exists": True}}
        )
        results = self.db_session.execute(stmt).scalars().all()
        self.assertTrue(len(results) == 10)

    def test_attr_not_exists(self):
        """Test not $exists on a simple attr."""
        stmt = apply_mql_filters(
            model_class=Customer,
            filters={"company": {"$exists": False}}
        )
        results = self.db_session.execute(stmt).scalars().all()
        self.assertTrue(len(results) == 49)

    def test_child_list_not_exists(self):
        """Test a child list can be filtered for being missing."""
        stmt = apply_mql_filters(
            model_class=Artist,
            filters={"albums": {"$exists": False}}
        )
        results = self.db_session.execute(stmt).scalars().all()
        self.assertTrue(len(results) == 71)

    def test_child_list_exists(self):
        """Test a child list can be checked for existence."""
        stmt = apply_mql_filters(
            model_class=Artist,
            filters={"albums": {"$exists": True}}
        )
        results = self.db_session.execute(stmt).scalars().all()
        self.assertTrue(len(results) == 204)

    def test_child_non_list_not_exists(self):
        """Test a non list child can be filtered for being missing."""
        stmt = apply_mql_filters(
            model_class=Employee,
            filters={"manager": {"$exists": False}}
        )
        results = self.db_session.execute(stmt).scalars().all()
        self.assertTrue(len(results) == 1)

    def test_child_non_list_exists(self):
        """Test a non list child can be checked for existence."""
        stmt = apply_mql_filters(
            model_class=Employee,
            filters={"manager": {"$exists": True}}
        )
        results = self.db_session.execute(stmt).scalars().all()
        self.assertTrue(len(results) == 7)

    def test_implicit_and(self):
        """Test that an implicit and query works."""
        stmt = apply_mql_filters(
            model_class=Playlist,
            filters={
                "tracks.track_id": 7,
                "playlist_id": 1
            }
        )
        result = self.db_session.execute(stmt).scalars().all()
        self.assertTrue(len(result) == 1)
        self.assertTrue(result[0].playlist_id == 1)

    def test_explicit_and(self):
        """Test that the $and operator works."""
        stmt = apply_mql_filters(
            model_class=Playlist,
            filters={
                "$and": [
                    {"tracks.track_id": 7},
                    {"playlist_id": 1}
                ]
            }
        )
        result = self.db_session.execute(stmt).scalars().all()
        self.assertTrue(len(result) == 1)
        self.assertTrue(result[0].playlist_id == 1)

    def test_or(self):
        """Test that the $or operator works."""
        stmt = apply_mql_filters(
            model_class=Playlist,
            filters={
                "$or": [
                    {"tracks.track_id": 999999},
                    {"playlist_id": 1}
                ]
            }
        )
        result = self.db_session.execute(stmt).scalars().all()
        self.assertTrue(len(result) == 1)
        self.assertTrue(result[0].playlist_id == 1)

    def test_negation(self):
        """Test that the $not operator works."""
        stmt = apply_mql_filters(
            model_class=Playlist,
            filters={
                "tracks.track_id": 7,
                "$not": {
                    "playlist_id": 1
                }
            }
        )
        result = self.db_session.execute(stmt).scalars().all()
        self.assertTrue(len(result) == 1)
        self.assertTrue(result[0].playlist_id == 8)

    def test_nor(self):
        """Test that the $nor operator works."""
        stmt = apply_mql_filters(
            model_class=Playlist,
            filters={
                "tracks.track_id": 7,
                "$nor": [
                    {"playlist_id": 1},
                    {"playlist_id": 999}
                ]
            }
        )
        result = self.db_session.execute(stmt).scalars().all()
        self.assertTrue(len(result) == 1)
        self.assertTrue(result[0].playlist_id == 8)

    def test_neq(self):
        """Test that the $ne operator works."""
        stmt = apply_mql_filters(
            model_class=Playlist,
            filters={
                "tracks.track_id": 7,
                "playlist_id": {
                    "$ne": 1
                }
            }
        )
        result = self.db_session.execute(stmt).scalars().all()
        self.assertTrue(len(result) == 1)
        self.assertTrue(result[0].playlist_id == 8)

    def test_lt(self):
        """Test that the $lt operator works."""
        stmt = apply_mql_filters(
            model_class=Playlist,
            filters={
                "playlist_id": {
                    "$lt": 2
                }
            }
        )
        result = self.db_session.execute(stmt).scalars().all()
        self.assertTrue(len(result) == 1)
        self.assertTrue(result[0].playlist_id == 1)

    def test_lte(self):
        """Test that the $lte operator works."""
        stmt = apply_mql_filters(
            model_class=Playlist,
            filters={
                "playlist_id": {
                    "$lte": 1
                }
            }
        )
        result = self.db_session.execute(stmt).scalars().all()
        self.assertTrue(len(result) == 1)
        self.assertTrue(result[0].playlist_id == 1)

    def test_eq(self):
        """Test that the new $eq operator works."""
        stmt = apply_mql_filters(
            model_class=Playlist,
            filters={
                "playlist_id": {
                    "$eq": 1
                }
            }
        )
        result = self.db_session.execute(stmt).scalars().all()
        self.assertTrue(len(result) == 1)
        self.assertTrue(result[0].playlist_id == 1)

    def test_gte(self):
        """Test that the $gte operator works."""
        stmt = apply_mql_filters(
            model_class=Playlist,
            filters={
                "playlist_id": {
                    "$gte": 18
                }
            }
        )
        result = self.db_session.execute(stmt).scalars().all()
        self.assertTrue(len(result) == 1)
        self.assertTrue(result[0].playlist_id == 18)

    def test_gt(self):
        """Test that the $gt operator works."""
        stmt = apply_mql_filters(
            model_class=Playlist,
            filters={
                "playlist_id": {
                    "$gt": 17
                }
            }
        )
        result = self.db_session.execute(stmt).scalars().all()
        self.assertTrue(len(result) == 1)
        self.assertTrue(result[0].playlist_id == 18)

    def test_mod(self):
        """Test that the $mod operator works."""
        stmt = apply_mql_filters(
            model_class=Playlist,
            filters={
                "playlist_id": {
                    "$mod": [18, 0]
                }
            }
        )
        result = self.db_session.execute(stmt).scalars().all()
        self.assertTrue(len(result) == 1)
        self.assertTrue(result[0].playlist_id == 18)

    def test_mod_str_fail(self):
        """Test passing string values to $mod op fails."""
        self.assertRaises(
            InvalidMqlException,
            apply_mql_filters,
            model_class=Playlist,
            filters={
                "playlist_id": {
                    "$mod": ["test", "hey"]
                }
            }
        )

    def test_mod_decimal_divisor_fails(self):
        """Test passing a decimal divisor to $mod op fails."""
        self.assertRaises(
            InvalidMqlException,
            apply_mql_filters,
            model_class=Playlist,
            filters={
                "playlist_id": {
                    "$mod": [2.2, 4]
                }
            }
        )

    def test_mod_decimal_remainder_fails(self):
        """Test passing a decimal remainder to $mod op fails."""
        self.assertRaises(
            InvalidMqlException,
            apply_mql_filters,
            model_class=Playlist,
            filters={
                "playlist_id": {
                    "$mod": [2, 4.4]
                }
            }
        )

    def test_mod_non_list(self):
        """Test passing a non list to $mod op rails."""
        self.assertRaises(
            InvalidMqlException,
            apply_mql_filters,
            model_class=Playlist,
            filters={
                "playlist_id": {
                    "$mod": 5
                }
            }
        )

    def test_mod_non_int_field(self):
        """Test trying to $mod a non int field fails."""
        self.assertRaises(
            InvalidMqlException,
            apply_mql_filters,
            model_class=Playlist,
            filters={
                "name": {
                    "$mod": 5
                }
            }
        )

    def test_in(self):
        """Test that the $in operator works."""
        stmt = apply_mql_filters(
            model_class=Playlist,
            filters={
                "playlist_id": {
                    "$in": [1, 2]
                }
            }
        )
        result = self.db_session.execute(stmt).scalars().all()
        self.assertTrue(
            len(result) == 2)
        self.assertTrue(
            (result[0].playlist_id == 1 and result[1].playlist_id == 2) or
            (result[0].playlist_id == 2 and result[1].playlist_id == 1))

    def test_in_nested(self):
        """Test that the $in operator works on nested objects."""
        stmt = apply_mql_filters(
            model_class=Playlist,
            filters={
                "tracks.track_id": {
                    "$in": [7, 9999]
                }
            }
        )
        result = self.db_session.execute(stmt).scalars().all()
        self.assertTrue(
            len(result) == 2 and
            ((result[0].playlist_id == 1 and result[1].playlist_id == 8) or
             (result[0].playlist_id == 8 and result[1].playlist_id == 1)))

    def test_in_non_list_fails(self):
        """Test that the $in op fails when not supplied with a list."""
        self.assertRaises(
            InvalidMqlException,
            apply_mql_filters,
            model_class=Playlist,
            filters={
                "playlist_id": {
                    "$in": 1
                }
            }
        )

    def test_nin(self):
        """Test that the $nin operator works."""
        stmt = apply_mql_filters(
            model_class=Playlist,
            filters={
                "playlist_id": {
                    "$nin": [
                        2, 3, 4, 5, 6, 7, 8, 9, 10, 
                        11, 12, 13, 14, 15, 16, 17, 18
                    ]
                }
            }
        )
        result = self.db_session.execute(stmt).scalars().all()
        self.assertTrue(len(result) == 1 and
                        result[0].playlist_id == 1)

    def test_nin_non_list_fails(self):
        """Test that the $nin op fails when not supplied with a list."""
        self.assertRaises(
            InvalidMqlException,
            apply_mql_filters,
            model_class=Playlist,
            filters={
                "playlist_id": {
                    "$nin": 1
                }
            }
        )

    def test_like(self):
        """Test that the new $like operator works."""
        stmt = apply_mql_filters(
            model_class=Employee,
            filters={
                "first_name": {
                    "$like": "tev"
                }
            }
        )
        result = self.db_session.execute(stmt).scalars().all()
        self.assertTrue(len(result) == 1)
        self.assertTrue(result[0].first_name == "Steve")

    def test_elemmatch_fail(self):
        """Test that the $elemMatch operator properly fails."""
        self.assertRaises(
            InvalidMqlException,
            apply_mql_filters,
            model_class=Employee,
            filters={
                "first_name": {
                    "$elemMatch": {"test": "test"}
                }
            }
        )

    def test_nested_attr_query_fail(self):
        """Test that a nested attribute query fails."""
        self.assertRaises(
            InvalidMqlException,
            apply_mql_filters,
            model_class=Track,
            filters={
                "track_id": {
                    "info": 5
                }
            }
        )

    def test_bad_operator_fail(self):
        """Test that a invalid operator fails."""
        self.assertRaises(
            InvalidMqlException,
            apply_mql_filters,
            model_class=Track,
            filters={
                "track_id": {
                    "$bad": 5
                }
            },
            whitelist=["track_id"]
        )

    def test_empty_dict_fail(self):
        """Test that a nested attribute query fails."""
        self.assertRaises(
            InvalidMqlException,
            apply_mql_filters,
            model_class=Playlist,
            filters={"tracks": {}}
        )

    def test_whitelist(self):
        """Test that whitelisting works as expected."""
        self.assertRaises(
            InvalidMqlException,
            apply_mql_filters,
            model_class=Playlist,
            filters={"tracks.track_id": 7},
            whitelist=[]
        )
        self.assertFalse(
            mqlalchemy._is_whitelisted(
                Album,
                "bad_attr_name",
                ["bad_attr_name"]
            )
        )
        stmt = apply_mql_filters(
            model_class=Playlist,
            filters={"tracks.track_id": 7},
            whitelist=["tracks.track_id"]
        )
        result = self.db_session.execute(stmt).scalars().all()
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
        stmt = apply_mql_filters(
            model_class=Playlist,
            filters={"tracks.track_id": 7},
            whitelist=whitelist
        )
        result = self.db_session.execute(stmt).scalars().all()
        self.assertTrue(
            len(result) == 2)
        self.assertTrue(
            (result[0].playlist_id == 1 and result[1].playlist_id == 8) or
            (result[0].playlist_id == 8 and result[1].playlist_id == 1))
        self.assertRaises(
            InvalidMqlException,
            apply_mql_filters,
            model_class=Playlist,
            filters={"tracks.name": "Test"},
            whitelist=whitelist
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
            Album,
            "tracks.0.track_id")
        self.assertTrue(len(class_attrs) == 4)

    def test_stack_size_limit(self):
        """Make sure that limiting the stack size works as expected."""
        stmt = apply_mql_filters(
            model_class=Album,
            filters={
                "album_id": 1,
                "title": "For Those About To Rock We Salute You"},
            stack_size_limit=10
        )
        result = self.db_session.execute(stmt).scalars().all()
        self.assertTrue(len(result) == 1)
        self.assertTrue(result[0].album_id == 1)

    def test_stack_size_limit_fail(self):
        """Make sure that limiting the stack size fails as expected."""
        self.assertRaises(
            InvalidMqlException,
            apply_mql_filters,
            model_class=Album,
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
            model_class=Playlist,
            filters={"playlist_id": "test"}
        )

    def test_self_referential_relation(self):
        """Test relationship chain leading to the same model."""
        stmt = apply_mql_filters(
            model_class=Album,
            filters={"tracks.album.album_id": 18}
        )
        result = self.db_session.execute(stmt).scalars().all()
        self.assertTrue(len(result) == 1)
        self.assertTrue(result[0].album_id == 18)

    def test_required_filters(self):
        """Test nested conditions are applied properly."""
        nested_condition_log = {}

        def nested_conditions(key):
            """Return required filters for a relation based on key name.

            :param str key: Dot separated data key, relative to the
                root model.
            :return: Any required filters to be applied to the child
                relationship.

            """
            # Do some logging of how many times each key is hit
            nested_condition_log[key] = (nested_condition_log.get(key) or 0) + 1
            if key == "tracks":
                return Track.album.has(Album.album_id != 18)

        # Search playlist for a track that is explicitly excluded
        # via required_filters
        stmt = apply_mql_filters(
            model_class=Playlist,
            filters={"tracks.track_id": 166},
            nested_conditions=nested_conditions
        )
        self.assertTrue(nested_condition_log.get("tracks") == 1)
        result = self.db_session.execute(stmt).scalars().all()
        self.assertTrue(len(result) == 0)

    def test_required_filters_tuple(self):
        """Test nested conditions are applied properly as a tuple."""
        nested_condition_log = {}

        def nested_conditions(key):
            """Return required filters for a relation based on key name.

            :param str key: Dot separated data key, relative to the
                root model.
            :return: Any required filters to be applied to the child
                relationship.
            :rtype: tuple

            """
            # Do some logging of how many times each key is hit
            nested_condition_log[key] = (nested_condition_log.get(key) or 0) + 1
            if key == "tracks":
                return tuple([Track.album.has(Album.album_id != 18)])

        # Search playlist for a track that is explicitly excluded
        # via required_filters
        stmt = apply_mql_filters(
            model_class=Playlist,
            filters={"tracks.track_id": 166},
            nested_conditions=nested_conditions
        )
        self.assertTrue(nested_condition_log.get("tracks") == 1)
        result = self.db_session.execute(stmt).scalars().all()
        self.assertTrue(len(result) == 0)

    def test_nested_conditions_dict(self):
        """Test dict nested conditions are applied properly."""
        nested_conditions = {
            "tracks": (
                Track.album.has(Album.album_id != 18))}

        # Search playlist for a track that is explicitly excluded
        # via required_filters
        stmt = apply_mql_filters(
            model_class=Playlist,
            filters={"tracks.track_id": 166},
            nested_conditions=nested_conditions
        )
        result = self.db_session.execute(stmt).scalars().all()
        self.assertTrue(len(result) == 0)


if __name__ == '__main__':    # pragma no cover
    unittest.main()
