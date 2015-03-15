# mqlalchemy
[![Build Status](https://travis-ci.org/repole/mqlalchemy.svg?branch=master)](https://travis-ci.org/repole/mqlalchemy)
[![Coverage Status](https://coveralls.io/repos/repole/mqlalchemy/badge.svg?branch=master)](https://coveralls.io/r/repole/mqlalchemy?branch=master)

Query SQLAlchemy models using MongoDB style syntax.


**Why?**

The need arose for me to be able to pass complex database filters from client side JavaScript to a Python server. I started building some JSON style syntax to do so, then realized such a thing already existed. I've never seriously used MongoDB, but the syntax for querying lends itself pretty perfectly to this use case.


**That sounds pretty dangerous...**

It can be. When using this with any sort of user input, you'll want to pass in a whitelist of attributes that are ok to query, otherwise you'll open the possibility of leaked passwords and all sorts of other scary stuff.


**So, can I actually use this for a serious project?**

Maybe? There's some decent test coverage, but this certainly isn't a very mature project yet.

I'll be pretty active in supporting this, so if you are using this and run into problems, I should be pretty quick to fix them.

**Supported Operators**

* $and
* $or
* $not
* $nor
* $in
* $nin
* $gt
* $gte
* $lt
* $lte
* $ne
* $mod

Custom operators added for convenience:
* $eq - Explicit equality check.
* $like - Search a text field for the given value.

Not yet supported, but would like to add:
* Index based relation queries. Album.tracks.0.track_id won't work.
* $regex

**Examples?**
```python
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
# Find all albums with a track on the "Grunge" playlist or are by
# Led Zeppelin.
filters = {
    "$or": [
        {"tracks.playlists.name": "Grunge"},
        {"artist.name": "Led Zeppelin"}
    ]
}
query = apply_mql_filters(db_session, Album, filters, whitelist)
matching_records = query.all()
```

**How fast is it?**

I'm sure my actual syntax parsing is inefficient and has loads of room for improvement, but the time it takes to parse should be minimal compared to the actual database query, so this shouldn't slow your queries down too much.


**TODO**
* Include some more complex testing.
* Improve documentation.
* Split my one massive function into a more maintainable set of functions.

Certainly open to input and contributions if you'd like to help.
