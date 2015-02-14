import unittest
import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from mqlalchemy.tests import models
from mqlalchemy import apply_mql_filters


class MQLAlchemyTests(unittest.TestCase):

    def setUp(self):
        connect_string = "sqlite+pysqlite:///" + os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "chinook.sqlite")
        self.db_engine = create_engine(connect_string)
        self.DBSession = sessionmaker(bind=self.db_engine)
        self.db_session = self.DBSession()

    def test_db(self):
        # make sure our test db is functional
        result = self.db_session.query(models.Album).filter(
            models.Album.AlbumId == 1).all()
        self.assertTrue(len(result) == 1 and result[0].ArtistId == 1)

    def test_simple_query(self):
        # test a very simple mqlalchemy query
        query = apply_mql_filters(
            self.db_session,
            models.Album,
            {"AlbumId": 2}
        )
        result = query.all()
        self.assertTrue(
            len(result) == 1 and
            result[0].Title == "Balls to the Wall")

    def test_list_relation(self):
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


if __name__ == '__main__':
    unittest.main()