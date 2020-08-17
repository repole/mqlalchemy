MQLAlchemy
==========

|Build Status| |Coverage Status| |Docs|

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

**So, can I actually use this for a serious project?**

Maybe? There's some decent test coverage, but this certainly isn't a
very mature project yet.

I'll be pretty active in supporting this, so if you are using this and
run into problems, I should be pretty quick to fix them.

**How fast is it?**

I'm sure my actual syntax parsing is inefficient and has loads of room
for improvement, but the time it takes to parse should be minimal
compared to the actual database query, so this shouldn't slow your
queries down too much.

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
    query = apply_mql_filters(db_session, Album, filters, whitelist)
    matching_records = query.all()

For more, please see the included tests, as they're probably the
easiest way to get an idea of how the library can be used.

Contributing
------------

Submit a pull request and make sure to include an updated AUTHORS 
with your name along with an updated CHANGES.rst.

License
-------

MIT

.. |Build Status| image:: https://travis-ci.org/repole/mqlalchemy.svg?branch=master
   :target: https://travis-ci.org/repole/mqlalchemy
.. |Coverage Status| image:: https://coveralls.io/repos/repole/mqlalchemy/badge.svg?branch=master
   :target: https://coveralls.io/r/repole/mqlalchemy?branch=master
.. |Docs| image:: https://readthedocs.org/projects/mqlalchemy/badge/?version=latest
   :target: http://mqlalchemy.readthedocs.org/en/latest/

