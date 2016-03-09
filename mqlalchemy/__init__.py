## -*- coding: utf-8 -*-\
"""
    mqlalchemy.__init__
    ~~~~~~~~~~~~~~~~~~~

    Query SQLAlchemy objects using MongoDB style syntax.

    :copyright: (c) 2016 by Nicholas Repole and contributors.
                See AUTHORS for more details.
    :license: MIT - See LICENSE for more details.
"""
from __future__ import unicode_literals
from mqlalchemy._compat import str
from mqlalchemy.utils import dummy_gettext
import sqlalchemy
from sqlalchemy.orm import ColumnProperty, RelationshipProperty
from sqlalchemy.types import String, Text, Unicode, UnicodeText, Enum, \
    Integer, BigInteger, SmallInteger, Boolean, Date, DateTime, Float, \
    Numeric, Time, BIGINT, BOOLEAN, CHAR, CLOB, DATE, DATETIME, \
    DECIMAL, FLOAT, INT, INTEGER, NCHAR, NVARCHAR, NUMERIC, REAL, \
    SMALLINT, TEXT, TIME, TIMESTAMP, VARCHAR
from sqlalchemy.inspection import inspect
import datetime


__all__ = [b"apply_mql_filters", b"InvalidMQLException",
           b"convert_to_alchemy_type"]
__version__ = "0.2.0"


class InvalidMQLException(Exception):

    """Generic exception class for invalid queries."""

    pass


def apply_mql_filters(query_session, model_class, filters=None,
                      whitelist=None, stack_size_limit=None,
                      convert_key_names_func=None, gettext=None):
    """Applies filters to a query and returns it.

    Supported operators include:

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

    Considering adding:

    * $regex
    * $size
    * Array index queries - e.g. Album.tracks.0 to get the first track.

    Won't be implemented:

    * $all
    * $exists
    * $text
    * $type
    * $where
    * Exact matches for arrays/relationships

    This function is massive, but breaking it up seemed to make
    things even harder to follow. Should find a better way of
    breaking things up, but for now, accept my apologies.

    Args:
        query_session: A db session or query object. Filters are applied
            to this object.
        model_class: The sqlalchemy model class you want to query.
        filters: A dictionary of MongoDB style query filters.
        whitelist: May either be a function, a list, or `None`. If a
            function is provided, it should take in a dot separated
            field name and return `True` if it is acceptable to query
            that field, or `False` if not. If a list of fieldnames is
            provided, field names will be checked against that list
            to determine whether or not it is an allowed field to be
            queried. If `None` is provided, all fields and relationships
            of a model will be queryable. Also note that the field name
            being checked will already have been converted by
            convert_key_names_func if provided.
        convert_key_names_func: Optional function used to convert
            a provided attribute name into a field name for a model.
            Should take one parameter, which is a dot separated
            name, and should return a converted string in the same
            dot separated format. For example, say you want to
            be able to query your model, which contains field names
            with underscores, using lowerCamelCase instead. The
            provided function should take a string such as
            "tracks.unitPrice" and convert it to "tracks.unit_price".
            For the sake of raising more useful exceptions, the function
            should return `None` if an invalid field name is provided,
            however this is not necessary.
        stack_size_limit: Optional parameter used to limit the allowable
            complexity of the provided filters. Can be useful in
            preventing malicious query attempts.
        gettext: Supply a translation function to convert error messages
            to the desired language. Note that no translations are
            included by default, you must generate your own.

    """
    # TODO - Improve error messages
    if convert_key_names_func is None:
        def convert_key_names_func(x): return x
    if isinstance(whitelist, list):
        def is_whitelisted(attr_name):
            """Uses the default, built in whitelist checker."""
            return _is_whitelisted(model_class, attr_name, whitelist)
    elif callable(whitelist):
        def is_whitelisted(attr_name):
            """Uses the provided whitelist function."""
            return whitelist(attr_name)
    else:
        def is_whitelisted(attr_name):
            """All attributes will be queryable."""
            if attr_name:
                return True
    if gettext is None:
        gettext = dummy_gettext
    _ = gettext
    if hasattr(query_session, "query"):
        query = query_session.query(model_class)
    else:
        query = query_session
    if filters is not None:
        # NOTE: Any variable with a c_ prefix is used to store
        # converted key names, in accordance with convert_key_names
        # e.g. attr_name_stack = ["someAttr", "otherAttr"]
        # c_attr_name_stack = ["some_attr", "other_attr"]
        query_stack = list()
        c_attr_name_stack = list()
        attr_name_stack = list()
        sub_query_name_stack = list()
        c_sub_query_name_stack = list()
        relation_type_stack = list()
        query_tree_stack = list()
        query_tree_stack.append({
            "op": sqlalchemy.and_,
            "expressions": []
        })
        query_stack.append(filters)
        relation_type_stack.append(model_class)
        attr_name_stack.append(model_class.__name__)
        c_attr_name_stack.append(model_class.__name__)
        sub_query_name_stack.append(model_class.__name__)
        c_sub_query_name_stack.append(model_class.__name__)
        while query_stack:
            if stack_size_limit and len(query_stack) > stack_size_limit:
                raise InvalidMQLException(
                    "This query is too complex.")
            item = query_stack.pop()
            if isinstance(item, str):
                if item == "POP_attr_name_stack":
                    attr_name_stack.pop()
                    c_attr_name_stack.pop()
                elif item == "POP_sub_query_name_stack":
                    sub_query_name_stack.pop()
                    c_sub_query_name_stack.pop()
                    relation_type_stack.pop()
                elif item == "POP_query_tree_stack":
                    query_tree = query_tree_stack.pop()
                    if (query_tree["op"] == sqlalchemy.and_ or
                            query_tree["op"] == sqlalchemy.or_):
                        expression = query_tree["op"](
                            *query_tree["expressions"])
                    elif query_tree["op"] == sqlalchemy.not_:
                        expression = sqlalchemy.not_(
                            query_tree["expressions"][0])
                    else:
                        # should be a .has or .any.
                        # expressions should be a one element list
                        if len(query_tree[
                                "expressions"]) != 1:    # pragma no cover
                            # failsafe - Should never reach here.
                            raise InvalidMQLException(
                                _("Unexpected error. Too many binary " +
                                  "expressions for this operator."))
                        expression = query_tree["op"](
                            query_tree["expressions"][0])
                    query_tree_stack[-1]["expressions"].append(expression)
            if isinstance(item, dict):
                if len(item) > 1:
                    query_tree_stack.append({
                        "op": sqlalchemy.and_,
                        "expressions": []
                    })
                    query_stack.append("POP_query_tree_stack")
                    for key in item:
                        query_stack.append({key: item[key]})
                elif len(item) == 1:
                    # Given an attr stack ["Album", "tracks.playlists"]
                    # Current key of "playlist_id"
                    # Convert the key name using the full_attr_name,
                    # converting it, and chopping off the previous attr
                    # names that were in the attr stack from the start.
                    key = list(item.keys())[0]
                    if not key.startswith("$"):
                        full_attr_name = _get_full_attr_name(
                            attr_name_stack[1:], key)
                        c_full_attr_name = convert_key_names_func(
                            full_attr_name)
                        split_c_attr_name = c_full_attr_name.split(".")
                        c_key = ".".join(
                            split_c_attr_name[-len(key.split(".")):])
                    else:
                        c_key = None
                    if key == "$or" or key == "$and":
                        if key == "$or":
                            op_func = sqlalchemy.or_
                        else:
                            op_func = sqlalchemy.and_
                        query_tree_stack.append({
                            "op": op_func,
                            "expressions": []
                        })
                        query_stack.append("POP_query_tree_stack")
                        for sub_item in item[key]:
                            query_stack.append(sub_item)
                    elif key == "$not":
                        query_tree_stack.append({
                            "op": sqlalchemy.not_,
                            "expressions": []
                        })
                        query_stack.append("POP_query_tree_stack")
                        query_stack.append(item[key])
                    elif key == "$nor":
                        query_tree_stack.append({
                            "op": sqlalchemy.not_,
                            "expressions": []
                        })
                        query_stack.append("POP_query_tree_stack")
                        query_stack.append({"$or": item[key]})
                    elif key == "$elemMatch":
                        attr_name = ".".join(attr_name_stack)
                        parent_sub_query_names = ".".join(sub_query_name_stack)
                        c_attr_name = ".".join(c_attr_name_stack)
                        c_parent_sub_query_names = ".".join(
                            c_sub_query_name_stack)
                        # trim the parent sub query attrs from
                        # the beginning of the attr_name
                        # note that parent_sub_query_names will always
                        # at least contain model_class at the start.
                        c_sub_query_name = c_attr_name[len(
                            c_parent_sub_query_names) + 1:]
                        c_sub_query_name_stack.append(c_sub_query_name)
                        sub_query_name = attr_name[len(
                            parent_sub_query_names) + 1:]
                        sub_query_name_stack.append(sub_query_name)
                        query_stack.append("POP_sub_query_name_stack")
                        query_stack.append("POP_query_tree_stack")
                        query_stack.append(item[key])
                        # [1:] to chop model_class from start of name stack
                        class_attrs = _get_class_attributes(
                            model_class, ".".join(c_attr_name_stack[1:]))
                        sub_class = class_attrs[-1]
                        relation_type_stack.append(sub_class)
                        if (hasattr(sub_class, "property") and
                                isinstance(sub_class.property,
                                           RelationshipProperty)):
                            if not sub_class.property.uselist:
                                query_tree_stack.append({
                                    "op": sub_class.has,
                                    "expressions": []
                                })
                            else:
                                query_tree_stack.append({
                                    "op": sub_class.any,
                                    "expressions": []
                                })
                        else:
                            raise InvalidMQLException(
                                _("$elemMatch not applied to subobject: ") +
                                attr_name)
                    elif key.startswith("$"):
                        class_attrs = _get_class_attributes(
                            model_class,
                            ".".join(c_attr_name_stack[1:]))
                        if (class_attrs and
                                hasattr(class_attrs[-1], "property") and
                                isinstance(class_attrs[-1].property,
                                           ColumnProperty)):
                            target_type = type(
                                class_attrs[-1].property.columns[0].type)
                            attr = class_attrs[-1]
                        else:
                            # failsafe - should never hit this
                            # due to earlier checks
                            raise InvalidMQLException(
                                _("Relation can't be checked for equality: ") +
                                _get_full_attr_name(attr_name_stack, key))
                        if key == "$lt":
                            expression = attr < convert_to_alchemy_type(
                                item[key], target_type)
                        elif key == "$lte":
                            expression = attr <= convert_to_alchemy_type(
                                item[key], target_type)
                        elif key == "$eq":
                            expression = attr == convert_to_alchemy_type(
                                item[key], target_type)
                        elif key == "$ne":
                            expression = attr != convert_to_alchemy_type(
                                item[key], target_type)
                        elif key == "$gte":
                            expression = attr >= convert_to_alchemy_type(
                                item[key], target_type)
                        elif key == "$gt":
                            expression = attr > convert_to_alchemy_type(
                                item[key], target_type)
                        elif key == "$like":
                            expression = attr.like("%" + str(item[key]) + "%")
                        elif key == "$in" or key == "$nin":
                            if not isinstance(item[key], list):
                                raise InvalidMQLException(
                                    _("%(attr)s must contain a list: ",
                                      attr=key) +
                                    _get_full_attr_name(attr_name_stack))
                            converted_list = []
                            for value in item[key]:
                                converted_list.append(
                                    convert_to_alchemy_type(
                                        value, target_type))
                            expression = attr.in_(converted_list)
                            if key == "$nin":
                                expression = sqlalchemy.not_(expression)
                        elif key == "$mod":
                            if (isinstance(item[key], list) and
                                    len(item[key]) == 2):
                                try:
                                    divider = int(item[key][0])
                                    result = int(item[key][1])
                                except ValueError:
                                    raise InvalidMQLException(
                                        _("Non int $mod values supplied: ") +
                                        _get_full_attr_name(attr_name_stack))
                                expression = attr.op("%")(divider) == result
                            else:
                                raise InvalidMQLException(
                                    _("Invalid $mod values supplied: ") +
                                    _get_full_attr_name(attr_name_stack))
                        else:
                            raise InvalidMQLException(
                                _("Invalid operator %(operator)s",
                                  operator=key) +
                                _get_full_attr_name(attr_name_stack))
                        query_tree_stack[-1]["expressions"].append(expression)
                    elif is_whitelisted(_get_full_attr_name(
                            c_attr_name_stack[1:], c_key)):
                        if len(attr_name_stack) > len(sub_query_name_stack):
                            # nested attribute queries aren't allowed.
                            # this type of search implies an equality
                            # check on an object.
                            raise InvalidMQLException(
                                _("Nested attribute queries are not " +
                                  "allowed: ") +
                                _get_full_attr_name(attr_name_stack))
                        # Next couple blocks of code help us find
                        # the first new relationship property
                        # in our attr hierarchy.
                        # TODO - Do this in a more pythonic way.
                        # find the properties that are relationship
                        # properties in our attr hierarchy.
                        c_full_attr_name = _get_full_attr_name(
                            c_attr_name_stack, c_key)
                        full_attr_name = _get_full_attr_name(
                            attr_name_stack, key)
                        class_attrs = _get_class_attributes(
                            model_class, _get_full_attr_name(
                                c_attr_name_stack[1:], c_key))
                        c_split_full_attr = c_full_attr_name.split('.')
                        split_full_attr = full_attr_name.split('.')
                        relation_indexes = []
                        for i, class_attr in enumerate(class_attrs):
                            if (hasattr(class_attr, "property") and
                                    isinstance(class_attr.property,
                                               RelationshipProperty)):
                                if (i == len(class_attrs) - 1 or not
                                        split_full_attr[i+1][0].isdigit()):
                                    relation_indexes.append(i)
                        # find the properties that are relationships
                        # that already have subqueries in our
                        # attr hierarchy.
                        # psq stands for parent_sub_query
                        psq_attr_name = ".".join(c_sub_query_name_stack)
                        psq_class_attrs = _get_class_attributes(
                            model_class, ".".join(c_sub_query_name_stack[1:]))
                        psq_split_attr_name = psq_attr_name.split('.')
                        psq_relation_indexes = []
                        for i, class_attr in enumerate(psq_class_attrs):
                            if (hasattr(class_attr, "property") and
                                    isinstance(class_attr.property,
                                               RelationshipProperty)):
                                if (i == len(psq_class_attrs) - 1 or not
                                        psq_split_attr_name[i+1][0].isdigit()):
                                    psq_relation_indexes.append(i)
                        if len(psq_relation_indexes) == len(relation_indexes):
                            # There is no new relationship query
                            attr_name_stack.append(key)
                            c_attr_name_stack.append(c_key)
                            query_stack.append("POP_attr_name_stack")
                            if isinstance(item[key], dict):
                                query_stack.append(item[key])
                            else:
                                query_stack.append({"$eq": item[key]})
                        elif len(relation_indexes) > len(psq_relation_indexes):
                            # Parse out the next relation sub query
                            # e.g.
                            # full_attr_name =
                            # rclass.prop1.Relation1.prop2.Relation2.p2
                            # sub_query_name_stack =
                            # [rclass, prop1.Relation1]
                            # result:
                            # prop2.Releation2
                            new_relation_index = relation_indexes[len(
                                psq_relation_indexes)]
                            attr_name = ""
                            c_attr_name = ""
                            # get the last relation index from the psq
                            prior_relation_index = 0
                            if len(psq_relation_indexes) > 0:
                                prior_relation_index = psq_relation_indexes[-1]
                            for i in range(prior_relation_index + 1,
                                           new_relation_index + 1):
                                attr_name += split_full_attr[i]
                                c_attr_name += c_split_full_attr[i]
                                next_relation = new_relation_index
                                if i != next_relation:    # pragma no cover
                                    # failsafe - won't hit this until
                                    # list index based queries are
                                    # implemented.
                                    attr_name += "."
                                    c_attr_name += "."
                            attr_name_stack.append(attr_name)
                            c_attr_name_stack.append(c_attr_name)
                            query_stack.append("POP_attr_name_stack")
                            # now parse out any remaining property
                            # names.
                            # In the above example, this would be p2
                            sub_attr_name = ""
                            for i in range(new_relation_index + 1,
                                           len(split_full_attr)):
                                sub_attr_name += split_full_attr[i]
                                if i != (len(split_full_attr) - 1):
                                    sub_attr_name += "."
                            # below generated $elemMatches will end up
                            # appending the attr_name (that was already
                            # appended to the attr_name_stack) to the
                            # sub_query_name_stack.
                            if (new_relation_index == relation_indexes[-1] and
                                    isinstance(item[key], dict)):
                                if sub_attr_name != "":
                                    # querying a single attribute of this
                                    # relation.
                                    query_stack.append({"$elemMatch": {
                                        sub_attr_name: item[key]}})
                                else:
                                    if not len(item[key].keys()) > 0:
                                        # dictionary has no keys.
                                        # invalid query.
                                        raise InvalidMQLException(
                                            _("Attribute can't be compared " +
                                              "to an empty dictionary: ") +
                                            full_attr_name)
                                    else:
                                        # TODO - may also want to check
                                        # for invalid sub_keys. A bad
                                        # key at this point would throw
                                        # an exception we aren't
                                        # gracefully catching.
                                        query_tree_stack.append({
                                            "op": sqlalchemy.and_,
                                            "expressions": []
                                        })
                                        query_stack.append(
                                            "POP_query_tree_stack")
                                        for sub_key in item[key].keys():
                                            if sub_key == "$elemMatch":
                                                query_stack.append({
                                                    "$elemMatch":
                                                        item[key][sub_key]})
                                            else:
                                                query_stack.append({
                                                    "$elemMatch": {
                                                        sub_key: item[key]}})
                            else:
                                if (new_relation_index ==
                                        relation_indexes[-1] and
                                        sub_attr_name == ""):
                                    # item[key] is not a dict and there
                                    # is no sub_attr, so we're trying to
                                    # equality check a relation.
                                    raise InvalidMQLException(
                                        _("Relation can't be checked for " +
                                          "equality: ") + full_attr_name)
                                else:
                                    # must have a sub_attr, so turn into
                                    # an elemMatch for that sub_attr.
                                    query_stack.append({"$elemMatch": {
                                        sub_attr_name: item[key]}})
                    else:
                        raise InvalidMQLException(
                            _("%(attr)s is not a whitelisted attribute.",
                              attr=_get_full_attr_name(attr_name_stack))
                        )
        if query_tree_stack[-1]["expressions"]:
            query = query.filter(
                sqlalchemy.and_(*query_tree_stack[-1]["expressions"]))
    return query


def _get_full_attr_name(attr_name_stack, short_attr_name=None):
    """Join the attr_name_stack to get a full attribute name."""
    attr_name = ".".join(attr_name_stack)
    if short_attr_name:
        if attr_name != "":
            attr_name += "."
        attr_name += short_attr_name
    return attr_name


def _is_whitelisted(model_class, attr_name, whitelist):
    """Check if this attr_name is approved to be filtered or sorted."""
    try:
        _get_class_attributes(model_class, attr_name)
    except AttributeError:
        # model_class doesn't contain this attr_name,
        # therefor it can't be queried.
        return False
    split_attr = attr_name.split(".")
    # parse out any list indexes. List1.0.other becomes Lis1.other
    attr_name = [attr for attr in split_attr if not attr[0].isdigit()]
    attr_name = ".".join([str(name) for name in attr_name])
    if attr_name in whitelist:
        return True
    return False


def _get_class_attributes(model_class, attr_name):
    """Get info about each attr given a dot notation attr name.

    :raises: AttributeError if an invalid attribute name is given.
    :returns: A list of attributes corresponding to the given
              attr_name for the provided model_class.

    """
    split_attr_name = attr_name.split(".")
    # We assume the full attr name includes the model_class
    # Thus we pop the first name.
    # e.g. model_class.prop.subprop becomes prop.subprop
    class_attrs = []
    root_type = model_class
    class_attrs.append(root_type)
    if len(split_attr_name) == 1 and split_attr_name[0] == '':
        # empty attr_name was provided
        return class_attrs
    elif len(split_attr_name) > 0:
        for attr_name in split_attr_name:
            if (hasattr(root_type, "property") and
                    isinstance(root_type.property,
                               RelationshipProperty)):
                if len(attr_name) > 0 and (
                        attr_name[0].isdigit()):
                    class_attrs.append(inspect(root_type).mapper.class_)
                    continue
                else:
                    root_type = inspect(root_type).mapper.class_
            # will raise an AttributeError if attr_name not in root_type
            class_attr = getattr(root_type, attr_name)
            root_type = class_attr
            class_attrs.append(class_attr)
    return class_attrs


def convert_to_alchemy_type(value, alchemy_type):
    """Convert a given value to a sqlalchemy friendly type."""
    text_types = [String, Unicode, Enum, Text, UnicodeText, CHAR, CLOB, NCHAR,
                  NVARCHAR, TEXT, VARCHAR]
    int_types = [Integer, BigInteger, SmallInteger, BIGINT, INT, INTEGER,
                 SMALLINT]
    bool_types = [Boolean, BOOLEAN]
    date_types = [Date, DATE]
    datetime_types = [DateTime, DATETIME, TIMESTAMP]
    float_types = [Float, Numeric, DECIMAL, FLOAT, NUMERIC, REAL]
    time_types = [Time, TIME]
    if value is None or str(value).lower() == "null":
        return None
    elif alchemy_type in int_types:
        if not isinstance(value, int):
            return int(value)
        else:
            return value
    elif alchemy_type in text_types:
        return str(value)
    elif alchemy_type in bool_types:
        if not isinstance(value, bool):
            if (str(value).lower() == "false" or
                    value == "0" or
                    value == 0 or
                    value is False):
                return False
            else:
                return True
        else:
            return value
    elif alchemy_type in date_types:
        if not isinstance(value, datetime.date):
            return datetime.datetime.strptime(value, '%Y-%m-%d').date()
        else:
            return value
    elif alchemy_type in datetime_types:
        if not isinstance(value, datetime.datetime):
            return datetime.datetime.strptime(value, '%Y-%m-%d %H:%M:%S')
        else:
            return value
    elif alchemy_type in float_types:
        if not isinstance(value, float):
            return float(value)
        else:
            return value
    elif alchemy_type in time_types:
        if not isinstance(value, datetime.time):
            return datetime.datetime.strptime(value, '%H:%M:%S').time()
        else:
            return value
    raise TypeError("Unable to convert value to alchemy type.")
