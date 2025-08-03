MQLAlchemy
==========

|Build Status| |Docs|

Query SQLAlchemy models using MongoDB style syntax.

Why?
----

The need arose for me to be able to pass complex database filters from
client side JavaScript to a Python server. I started building some JSON
style syntax to do so, then realized such a thing already existed. I've
never seriously used MongoDB, but the syntax for querying lends itself
pretty perfectly to this use case.

**That sounds pretty dangerous...**

It can be. When using this with any sort of user input, you'll want to
pass in a whitelist of attributes that are ok to query, as well as any
required filters for each model class, otherwise you'll open the
possibility of leaked passwords and all sorts of other scary stuff.

**How fast is it?**

The time it takes to parse should be minimal compared to the actual 
database query, so this shouldn't slow your queries down noticably.

Supported Operators
-------------------

-  $and
-  $or
-  $not
-  $nor
-  $in
-  $nin
-  $gt
-  $gte
-  $lt
-  $lte
-  $ne
-  $mod
-  $exists

Custom operators added for convenience: 

-  $eq - Explicit equality check.
-  $like - Search a text field for the given value.

Not yet supported, but would like to add:

-  Index based relation queries. Album.tracks.0.track_id won't work.
-  $regex

Examples
--------

.. code:: python

    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker
    from mqlalchemy import apply_mql_filters
    from myapp.mymodels import Album

    # get your sqlalchemy db session here
    db_engine = create_engine("sqlite+pysqlite:///mydb.sqlite")
    DBSession = sessionmaker(bind=db_engine)
    db_session = DBSession()

    # define which fields of Album are ok to query
    whitelist = ["album_id", "artist.name", "tracks.playlists.name"]
    # Find all albums that are either by Led Zeppelin or have a track 
    # that can be found on the "Grunge" playlist.
    filters = {
        "$or": [
            {"tracks.playlists.name": "Grunge"},
            {"artist.name": "Led Zeppelin"}
        ]
    }
    query = select(Album)
    query = apply_mql_filters(
        model_class=Album,
        query=select(Album),
        filters=filters, 
        whitelist=whitelist)
    matching_records = db_session.execute(query).scalars().all()

For more, please see the included tests, as they're probably the
easiest way to get an idea of how the library can be used.

Contributing
------------

Submit a pull request and make sure to include an updated AUTHORS 
with your name along with an updated CHANGES.rst.

License 
-------

MIT

.. |Docs| image:: https://readthedocs.org/projects/mqlalchemy/badge/?version=latest
   :target: http://mqlalchemy.readthedocs.org/en/latest/

.. |Build Status| image:: https://github.com/repole/mqlalchemy/actions/workflows/ci-cd.yml/badge.svg
   :target: https://github.com/repole/mqlalchemy/actions/workflows/ci-cd.yml
