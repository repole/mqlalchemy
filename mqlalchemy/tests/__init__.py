## -*- coding: utf-8 -*-\
"""
    mqlalchemy.tests.__init__
    ~~~~~

    Tests for our new query syntax.

    :copyright: (c) 2015 by Nicholas Repole.
    :license: BSD - See LICENSE for more details.
"""
import unittest
import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from mqlalchemy.tests import models
from mqlalchemy import apply_mql_filters, InvalidMQLException


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
            models.Album.AlbumId == 1).all()
        self.assertTrue(len(result) == 1 and result[0].ArtistId == 1)

    def test_simple_query(self):
        """Test a very simple mqlalchemy query."""
        query = apply_mql_filters(
            self.db_session,
            models.Album,
            {"AlbumId": 2}
        )
        result = query.all()
        self.assertTrue(
            len(result) == 1 and
            result[0].Title == "Balls to the Wall")

    def test_no_match(self):
        """Test that a query that should have no match works."""
        query = apply_mql_filters(
            self.db_session,
            models.Playlist,
            {"Track.TrackId": 7,
             "PlaylistId": 4}
        )
        result = query.all()
        self.assertTrue(len(result) == 0)

    def test_list_relation(self):
        """Test that a list relation .any query works."""
        query = apply_mql_filters(
            self.db_session,
            models.Playlist,
            {"Track.TrackId": 7}
        )
        result = query.all()
        self.assertTrue(
            len(result) == 2 and
            ((result[0].PlaylistId == 1 and result[1].PlaylistId == 8) or
             (result[0].PlaylistId == 8 and result[1].PlaylistId == 1)))

    def test_implicit_and(self):
        """Test that an implicit and query works."""
        query = apply_mql_filters(
            self.db_session,
            models.Playlist,
            {"Track.TrackId": 7,
             "PlaylistId": 1}
        )
        result = query.all()
        self.assertTrue(
            len(result) == 1 and
            result[0].PlaylistId == 1)

    def test_explicit_and(self):
        """Test that the $and operator works."""
        query = apply_mql_filters(
            self.db_session,
            models.Playlist,
            {"$and": [
                {"Track.TrackId": 7},
                {"PlaylistId": 1}
            ]}
        )
        result = query.all()
        self.assertTrue(
            len(result) == 1 and
            result[0].PlaylistId == 1)

    def test_or(self):
        """Test that the $or operator works."""
        query = apply_mql_filters(
            self.db_session,
            models.Playlist,
            {"$or": [
                {"Track.TrackId": 999999},
                {"PlaylistId": 1}
            ]}
        )
        result = query.all()
        self.assertTrue(
            len(result) == 1 and
            result[0].PlaylistId == 1)

    def test_negation(self):
        """Test that the $not operator works."""
        query = apply_mql_filters(
            self.db_session,
            models.Playlist,
            {"Track.TrackId": 7,
             "$not": {
                 "PlaylistId": 1
             }}
        )
        result = query.all()
        self.assertTrue(len(result) == 1 and
                        result[0].PlaylistId == 8)

    def test_nor(self):
        """Test that the $nor operator works."""
        query = apply_mql_filters(
            self.db_session,
            models.Playlist,
            {"Track.TrackId": 7,
             "$nor": [
                 {"PlaylistId": 1},
                 {"PlaylistId": 999}
             ]}
        )
        result = query.all()
        self.assertTrue(len(result) == 1 and
                        result[0].PlaylistId == 8)

    def test_neq(self):
        """Test that the $ne operator works."""
        query = apply_mql_filters(
            self.db_session,
            models.Playlist,
            {"Track.TrackId": 7,
             "PlaylistId": {
                 "$ne": 1
             }}
        )
        result = query.all()
        self.assertTrue(len(result) == 1 and
                        result[0].PlaylistId == 8)

    def test_lt(self):
        """Test that the $lt operator works."""
        query = apply_mql_filters(
            self.db_session,
            models.Playlist,
            {"PlaylistId": {
                "$lt": 2
            }}
        )
        result = query.all()
        self.assertTrue(len(result) == 1 and
                        result[0].PlaylistId == 1)

    def test_lte(self):
        """Test that the $lte operator works."""
        query = apply_mql_filters(
            self.db_session,
            models.Playlist,
            {"PlaylistId": {
                "$lte": 1
            }}
        )
        result = query.all()
        self.assertTrue(len(result) == 1 and
                        result[0].PlaylistId == 1)

    def test_eq(self):
        """Test that the new $eq operator works."""
        query = apply_mql_filters(
            self.db_session,
            models.Playlist,
            {"PlaylistId": {
                "$eq": 1
            }}
        )
        result = query.all()
        self.assertTrue(len(result) == 1 and
                        result[0].PlaylistId == 1)

    def test_gte(self):
        """Test that the $gte operator works."""
        query = apply_mql_filters(
            self.db_session,
            models.Playlist,
            {"PlaylistId": {
                "$gte": 18
            }}
        )
        result = query.all()
        self.assertTrue(len(result) == 1 and
                        result[0].PlaylistId == 18)

    def test_gt(self):
        """Test that the $gt operator works."""
        query = apply_mql_filters(
            self.db_session,
            models.Playlist,
            {"PlaylistId": {
                "$gt": 17
            }}
        )
        result = query.all()
        self.assertTrue(len(result) == 1 and
                        result[0].PlaylistId == 18)

    def test_mod(self):
        """Test that the $mod operator works."""
        query = apply_mql_filters(
            self.db_session,
            models.Playlist,
            {"PlaylistId": {
                "$mod": [18, 0]
            }}
        )
        result = query.all()
        self.assertTrue(len(result) == 1 and
                        result[0].PlaylistId == 18)

    def test_in(self):
        """Test that the $in operator works."""
        query = apply_mql_filters(
            self.db_session,
            models.Playlist,
            {"PlaylistId": {
                "$in": [1, 2]
            }}
        )
        result = query.all()
        self.assertTrue(
            len(result) == 2 and
            ((result[0].PlaylistId == 1 and result[1].PlaylistId == 2) or
             (result[0].PlaylistId == 2 and result[1].PlaylistId == 1)))
        query = apply_mql_filters(
            self.db_session,
            models.Playlist,
            {"Track.TrackId": {
                "$in": [7, 9999]
            }}
        )
        result = query.all()
        self.assertTrue(
            len(result) == 2 and
            ((result[0].PlaylistId == 1 and result[1].PlaylistId == 8) or
             (result[0].PlaylistId == 8 and result[1].PlaylistId == 1)))

    def test_nin(self):
        """Test that the $nin operator works."""
        query = apply_mql_filters(
            self.db_session,
            models.Playlist,
            {"PlaylistId": {
                "$nin": [
                    2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18]
            }}
        )
        result = query.all()
        self.assertTrue(len(result) == 1 and
                        result[0].PlaylistId == 1)

    def test_like(self):
        """Test that the new $like operator works."""
        query = apply_mql_filters(
            self.db_session,
            models.Employee,
            {"FirstName": {
                "$like": "tev"
            }}
        )
        result = query.all()
        self.assertTrue(len(result) == 1 and
                        result[0].FirstName == "Steve")

    def test_whitelist(self):
        """Test that whitelisting works as expected."""
        try:
            query = apply_mql_filters(
                self.db_session,
                models.Playlist,
                {"Track.TrackId": 7},
                []
            )
            self.assertTrue(False)
        except InvalidMQLException:
            self.assertTrue(True)
        query = apply_mql_filters(
            self.db_session,
            models.Playlist,
            {"Track.TrackId": 7},
            ["Track.TrackId"]
        )
        result = query.all()
        self.assertTrue(
            len(result) == 2 and
            ((result[0].PlaylistId == 1 and result[1].PlaylistId == 8) or
             (result[0].PlaylistId == 8 and result[1].PlaylistId == 1)))

if __name__ == '__main__':
    unittest.main()
