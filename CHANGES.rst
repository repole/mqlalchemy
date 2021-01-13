=======
Changes
=======

Release 0.4.0
=============

Features added
--------------
* ``$exists`` now a usable operator.


Release 0.3.0
=============

Incompatible changes
--------------------
* Python 2 support removed.
* Version requirement for SQLAlchemy now >= 1.0.
* ``InvalidMQLException`` renamed to ``InvalidMqlException``.

Features added
--------------
* May now pass ``nested_conditions`` to help ease enforcing read permissions.
* Separate out parsing of filters in case user doesn't want to immediately
  apply them.
* Added more descriptive error classes.
* Wrapped core functionality in a class to allow for easier overriding.
* Improved test coverage.

Documentation
-------------
* Changed docstring format back to standard Sphinx format.


Release 0.2.0
=============

Incompatible changes
--------------------
* convert_key_names parameter for ``apply_mql_filters`` removed.
* convert_key_names_func parameter for ``apply_mql_filters`` added.
* All ``RecordClass`` parameters names changed to ``model_class``.

Features added
--------------
* May also now pass in a function instead of a whitelist.

Documentation
-------------
* Changed docstring format to match Google's style guide.


Release 0.1.4
=============

Features added
--------------
* Ability to convert camelCase search parameters to underscore.
* Internationalization support for error messages.

Documentation
-------------
* Removed now broken badges.


Release 0.1.3
=============

Incompatible changes
--------------------
* Changed license from BSD to MIT.

Documentation
-------------
* Added more badges.
* Included install instructions for installing from source.


Release 0.1.2
=============

Features added
--------------
* readthedocs support

Bugs fixed
----------
* Model relationship attributes were causing documentation issues.
  Changed a few imports to work around the issue.

Documentation
-------------
* Added basic Sphinx documentation.