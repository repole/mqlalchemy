# mqlalchemy

Query SQLAlchemy models using MongoDB style syntax.


**Why?**

The need arose for me to be able to pass complex database filters from client side JavaScript to a Python server. I started building some JSON style syntax to do so, then realized such a thing already existed. I've never seriously used MongoDB, but the syntax for querying lends itself pretty perfectly to this use case.


**That sounds pretty dangerous...**

It can be. When using this with any sort of user input, you'll want to pass in a whitelist of attributes that are ok to query, otherwise you'll open the possibility of leaked passwords and all sorts of other scary stuff.


**So, can I actually use this for a serious project?**

You definitely shouldn't yet. I've barely tested this at all, lots of things are probably broken.


**Examples?**
```python
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from mqlalchemy import apply_mql_filters
from myapp.mymodels import User

# get your sqlalchemy db session here
db_engine = create_engine("sqlite+pysqlite:///mydb.sqlite")
DBSession = sessionmaker(bind=db_engine)
db_session = DBSession()

# define which fields of User are ok to query
whitelist = ["user_id", "username", "friends.user_id"]
# get back a user who's id is 1 and who has a friend with id 2
filters = {"user_id": 1, "friends.user_id": 2}
query = apply_mql_filters(db_session, User, filters, whitelist)
matching_records = query.all()
```

**How fast is it?**

I'm sure my actual syntax parsing is inefficient and has loads of room for improvement, but the time it takes to parse should be minimal compared to the actual database query, so this shouldn't slow your queries down too much.


**TODO**
* More tests
* Better documentation
* Split my one massive function into a more maintainable set of functions

Certainly open to input and contributions if you'd like to help.
