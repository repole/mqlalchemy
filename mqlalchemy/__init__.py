## -*- coding: utf-8 -*-\
"""
    mqlalchemy.__init__
    ~~~~~~~~~~~~~~~~~~~

    Query SQLAlchemy objects using MongoDB style syntax.

    :copyright: (c) 2015 by Nicholas Repole and contributors.
                See AUTHORS for more details.
    :license: BSD - See LICENSE for more details.
"""
from __future__ import unicode_literals
from mqlalchemy._compat import str
import sqlalchemy
from sqlalchemy.orm import ColumnProperty, RelationshipProperty
from sqlalchemy.types import String, Text, Unicode, UnicodeText, Enum, \
    Integer, BigInteger, SmallInteger, Boolean, Date, DateTime, Float, \
    Numeric, Time, BIGINT, BOOLEAN, CHAR, CLOB, DATE, DATETIME, \
    DECIMAL, FLOAT, INT, INTEGER, NCHAR, NVARCHAR, NUMERIC, REAL, \
    SMALLINT, TEXT, TIME, TIMESTAMP, VARCHAR
from sqlalchemy.inspection import inspect
import datetime

__version__ = "0.1.2"


class InvalidMQLException(Exception):

    """Generic exception class for invalid queries."""

    pass


def apply_mql_filters(query_session, RecordClass, filters=None,
                      whitelist=None, stack_size_limit=None):
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

    :param query_session: A db session or query object. Filters are
                          applied to this object.
    :param RecordClass: The sqlalchemy model class you want to query.
    :param filters: A dictionary of mongodb style query filters.
    :param whitelist: A list of the attributes that are approved for
                      filtering. If you are querying the User model,
                      your whitelist might include
                      ["username", "user_id", "sessions.session_id"]
                      while not including any fields related to
                      passwords or that type of thing.
                      If left as `None`, all attributes will be
                      queryable.
    :param stack_size_limit: Optoinal paramater used to limit the
                             allowable complexity of the provided
                             filters. Can be useful in proventing
                             malicious query attempts.

    """
    if hasattr(query_session, "query"):
        query = query_session.query(RecordClass)
    else:
        query = query_session
    if filters is not None:
        query_stack = list()
        attr_name_stack = list()
        sub_query_name_stack = list()
        relation_type_stack = list()
        query_tree_stack = list()
        query_tree_stack.append({
            "op": sqlalchemy.and_,
            "expressions": []
        })
        query_stack.append(filters)
        relation_type_stack.append(RecordClass)
        attr_name_stack.append(RecordClass.__name__)
        sub_query_name_stack.append(RecordClass.__name__)
        while query_stack:
            if stack_size_limit and len(query_stack) > stack_size_limit:
                raise InvalidMQLException(
                    "This query is too complex.")
            item = query_stack.pop()
            if isinstance(item, str):
                if item == "POP_attr_name_stack":
                    attr_name_stack.pop()
                elif item == "POP_sub_query_name_stack":
                    sub_query_name_stack.pop()
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
                                "Unexpected error. Too many binary " +
                                "expressions for this operator.")
                        expression = query_tree["op"](
                            query_tree["expressions"][0])
                    query_tree_stack[-1]["expressions"].append(expression)
            if isinstance(item, dict):
                if len(item.keys()) > 1:
                    query_tree_stack.append({
                        "op": sqlalchemy.and_,
                        "expressions": []
                    })
                    query_stack.append("POP_query_tree_stack")
                    for key in item:
                        query_stack.append({key: item[key]})
                elif len(item.keys()) == 1:
                    key = list(item.keys())[0]
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
                        # trim the parent sub query attrs from
                        # the beginning of the attr_name
                        # note that parent_sub_query_names will always
                        # at least contain RecordClass at the start.
                        sub_query_name = attr_name[len(
                            parent_sub_query_names) + 1:]
                        sub_query_name_stack.append(sub_query_name)
                        query_stack.append("POP_sub_query_name_stack")
                        query_stack.append("POP_query_tree_stack")
                        # query_stack.append(")")
                        query_stack.append(item[key])
                        class_attrs = _get_class_attributes(
                            RecordClass, attr_name)
                        SubClass = class_attrs[-1]
                        relation_type_stack.append(SubClass)
                        if (hasattr(SubClass, "property") and
                                type(SubClass.property) ==
                                RelationshipProperty):
                            if not SubClass.property.uselist:
                                query_tree_stack.append({
                                    "op": SubClass.has,
                                    "expressions": []
                                })
                            else:
                                query_tree_stack.append({
                                    "op": SubClass.any,
                                    "expressions": []
                                })
                        else:
                            raise InvalidMQLException(
                                "$elemMatch not applied to subobject: " +
                                attr_name)
                    elif key.startswith("$"):
                        class_attrs = _get_class_attributes(
                            RecordClass,
                            ".".join(attr_name_stack))
                        if (class_attrs and
                                hasattr(class_attrs[-1], "property") and
                                type(class_attrs[-1].property) ==
                                ColumnProperty):
                            target_type = type(
                                class_attrs[-1].property.columns[0].type)
                            attr = class_attrs[-1]
                        else:
                            # failsafe - should never hit this
                            # due to earlier checks
                            raise InvalidMQLException(
                                "Relation can't be checked for equality: " +
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
                                    key + " must contain a list: " +
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
                                        "Non int $mod values supplied: " +
                                        _get_full_attr_name(attr_name_stack))
                                expression = attr.op("%")(divider) == result
                            else:
                                raise InvalidMQLException(
                                    "Invalid $mod values supplied: " +
                                    _get_full_attr_name(attr_name_stack))
                        else:
                            raise InvalidMQLException(
                                "Invalid operator " + key +
                                _get_full_attr_name(attr_name_stack))
                        query_tree_stack[-1]["expressions"].append(expression)
                    elif _is_whitelisted(RecordClass,
                                         _get_full_attr_name(
                                             attr_name_stack, key),
                                         whitelist):
                        if len(attr_name_stack) > len(sub_query_name_stack):
                            # nested attribute queries aren't allowed.
                            # this type of search implies an equality
                            # check on an object.
                            raise InvalidMQLException(
                                "Nested attribute queries are not allowed: " +
                                _get_full_attr_name(attr_name_stack))
                        # Next couple blocks of code help us find
                        # the first new relationship property
                        # in our attr hierarchy.
                        # TODO - Do this in a more pythonic way.
                        # find the properties that are relationship
                        # properties in our attr hierarchy.
                        full_attr_name = _get_full_attr_name(
                            attr_name_stack, key)
                        class_attrs = _get_class_attributes(
                            RecordClass, full_attr_name)
                        split_full_attr = full_attr_name.split('.')
                        relation_indexes = []
                        for i, class_attr in enumerate(class_attrs):
                            if (hasattr(class_attr, "property") and
                                    type(class_attr.property) ==
                                    RelationshipProperty):
                                if (i == len(class_attrs) - 1 or not
                                        split_full_attr[i+1][0].isdigit()):
                                    relation_indexes.append(i)
                        # find the properties that are relationships
                        # that already have subqueries in our
                        # attr hierarchy.
                        # psq stands for parent_sub_query
                        psq_attr_name = ".".join(sub_query_name_stack)
                        psq_class_attrs = _get_class_attributes(
                            RecordClass, psq_attr_name)
                        psq_split_attr_name = psq_attr_name.split('.')
                        psq_relation_indexes = []
                        for i, class_attr in enumerate(psq_class_attrs):
                            if (hasattr(class_attr, "property") and
                                    type(class_attr.property) ==
                                    RelationshipProperty):
                                if (i == len(psq_class_attrs) - 1 or not
                                        psq_split_attr_name[i+1][0].isdigit()):
                                    psq_relation_indexes.append(i)
                        if len(psq_relation_indexes) == len(relation_indexes):
                            # There is no new relationship query
                            attr_name_stack.append(key)
                            query_stack.append("POP_attr_name_stack")
                            if isinstance(item[key], dict):
                                query_stack.append(item[key])
                            else:
                                query_stack.append({"$eq": item[key]})
                        elif len(relation_indexes) > len(psq_relation_indexes):
                            # Parse out the next relation sub query
                            # e.g.
                            # full_attr_name =
                            # RClass.prop1.Relation1.prop2.Relation2.p2
                            # sub_query_name_stack =
                            # [RClass, prop1.Relation1]
                            # result:
                            # prop2.Releation2
                            new_relation_index = relation_indexes[len(
                                psq_relation_indexes)]
                            attr_name = ""
                            # get the last relation index from the psq
                            prior_relation_index = 0
                            if len(psq_relation_indexes) > 0:
                                prior_relation_index = psq_relation_indexes[-1]
                            for i in range(prior_relation_index + 1,
                                           new_relation_index + 1):
                                attr_name += split_full_attr[i]
                                next_relation = new_relation_index
                                if i != next_relation:    # pragma no cover
                                    # failsafe - won't hit this until
                                    # list index based queries are
                                    # implemented.
                                    attr_name += "."
                            attr_name_stack.append(attr_name)
                            query_stack.append("POP_attr_name_stack")
                            # now parse out any remaining property names.
                            # in the above example, this would be p2
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
                                        # dictionary has no keys, invalid query.
                                        raise InvalidMQLException(
                                            "Attribute " + full_attr_name +
                                            " can't be compared to an empty " +
                                            " dictionary.")
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
                                        "Relation " + full_attr_name +
                                        " can't be checked for equality.")
                                else:
                                    # must have a sub_attr, so turn into
                                    # an elemMatch for that sub_attr.
                                    query_stack.append({"$elemMatch": {
                                        sub_attr_name: item[key]}})
                    else:
                        raise InvalidMQLException(
                            _get_full_attr_name(attr_name_stack) +
                            " is not a whitelisted attribute."
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


def _is_whitelisted(RecordClass, attr_name, whitelist):
    """Check if this attr_name is approved to be filtered or sorted."""
    try:
        _get_class_attributes(RecordClass, attr_name)
    except AttributeError:
        # RecordClass doesn't contain this attr_name,
        # therefor it can't be queried.
        return False
    if whitelist is None:
        return True
    attr_name = str(attr_name)
    if attr_name.startswith(RecordClass.__name__):
        attr_name = attr_name[(len(RecordClass.__name__) + 1):]
        split_attr = attr_name.split(".")
        # parse out any list indexes. List1.0.other becomes Lis1.other
        attr_name = [attr for attr in split_attr if not attr[0].isdigit()]
        attr_name = ".".join([str(name) for name in attr_name])
        if attr_name in whitelist:
            return True
    return False


def _get_class_attributes(RecordClass, attr_name):
    """Get info about each attr given a dot notation attr name.

    :raises: AttributeError if an invalid attribute name is given.
    :returns: A list of attributes corresponding to the given
              attr_name for the provided RecordClass.

    """
    split_attr_name = attr_name.split(".")
    # We assume the full attr name includes the RecordClass
    # Thus we pop the first name.
    # e.g. RecordClass.prop.subprop becomes prop.subprop
    split_attr_name.pop(0)
    class_attrs = []
    root_type = RecordClass
    class_attrs.append(root_type)
    if len(split_attr_name) > 0:
        for attr_name in split_attr_name:
            if (hasattr(root_type, "property") and
                    type(root_type.property) == RelationshipProperty):
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
