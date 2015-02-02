## -*- coding: utf-8 -*-\
"""
    mqlalchemy.__init__
    ~~~~~

    Query SQLAlchemy objects using MongoDB style syntax.

    :copyright: (c) 2015 by Nicholas Repole.
    :license: BSD - See LICENSE.md for more details.
"""
from mqlalchemy._compat import str
from sqlalchemy import and_, or_, not_
from sqlalchemy.orm import class_mapper, ColumnProperty, RelationshipProperty
from sqlalchemy.sql.elements import BinaryExpression
from sqlalchemy.types import String, Text, Unicode, UnicodeText, Enum, \
    Integer, BigInteger, SmallInteger, Boolean, Date, DateTime, Float, \
    Numeric, Time, BIGINT, BINARY, BLOB, BOOLEAN, CHAR, CLOB, DATE, \
    DATETIME, DECIMAL, FLOAT, INT, INTEGER, NCHAR, NVARCHAR, NUMERIC, \
    REAL, SMALLINT, TEXT, TIME, TIMESTAMP, VARBINARY, VARCHAR
from sqlalchemy.inspection import inspect
import datetime

__version__ = "0.1.0"

class InvalidMQLException(Exception):
    """Generic exception class for invalid queries."""
    pass

def apply_mql_filters(RecordClass, query_session, filters=None,
                      whitelist=None):
    """Applies filters to a query and returns it.

    Supported operators include:
    $and
    $or
    $not
    $nor
    $in
    $nin
    $gt
    $gte
    $in
    $lt
    $lte
    $ne
    $mod
    $all

    Custom operators added for convenience:
    $eq - Explicit equality check.
    $like - Search a text field for the given value.

    Considering adding:
    $regex
    $size
    Array index queries - User.sessions.0 would query the first session
                          for that user. Thus far haven't been able to
                          find a way to support this with sqlalchemy.

    Won't be implemented:
    $exists
    $text
    $type
    $where
    Exact matches for arrays/relationships

    This function is massive, but breaking it up seemed to make
    things even harder to follow. Should find a better way of
    breaking things up, but for now, accept my apologies.

    :param RecordClass: The sqlalchemy model class you want to query.
    :param query_session: A db session or query object. Filters are
                          applied to this object.
    :param filters: A dictionary of mongodb style query filters.
    :param whitelist: A list of the attributes that are approved for
                      filtering. If you are querying the User model,
                      your whitelist might include:
                      ["username", "user_id", "sessions.session_id"]
                      While not including any fields related to
                      passwords or that type of thing.
                      If left as `None`, all attributes will be
                      queryable.

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
            "op": and_,
            "expressions": []
        })
        query_stack.append(filters)
        relation_type_stack.append(RecordClass)
        attr_name_stack.append(RecordClass.__name__)
        sub_query_name_stack.append(RecordClass.__name__)
        while query_stack:
            item = query_stack.pop()
            if isinstance(item, str):
                if item == "POP_attr_name_stack":
                    attr_name_stack.pop()
                elif item == "POP_sub_query_name_stack":
                    sub_query_name_stack.pop()
                    relation_type_stack.pop()
                elif item == "POP_query_tree_stack":
                    query_tree = query_tree_stack.pop()
                    if (query_tree["op"] == and_ or
                            query_tree["op"] == or_):
                        expression = query_tree["op"](
                            *query_tree["expressions"])
                    elif query_tree["op"] == not_:
                        expression = not_(query_tree["expressions"][0])
                    else:
                        #should be a .has or .any.
                        #expressions should be a one element list
                        #TODO - check expressions len is 1, else raise
                        expression = query_tree["op"](
                            query_tree["expressions"][0])
                    query_tree_stack[-1]["expressions"].append(expression)
            if isinstance(item, dict):
                if len(item.keys()) > 1:
                    query_tree_stack.append({
                        "op": and_,
                        "expressions": []
                    })
                    query_stack.append("POP_query_tree_stack")
                    for key in item:
                        query_stack.append({key: item[key]})
                elif len(item.keys()) == 1:
                    key = list(item.keys())[0]
                    if key == "$or" or key == "$and":
                        if key == "$or":
                            op_func = or_
                        else:
                            op_func = and_
                        query_tree_stack.append({
                            "op": op_func,
                            "expressions": []
                        })
                        query_stack.append("POP_query_tree_stack")
                        for sub_item in item[key]:
                            query_stack.append(sub_item)
                    elif key == "$not":
                        query_tree_stack.append({
                            "op": not_,
                            "expressions": []
                        })
                        query_stack.append("POP_query_tree_stack")
                        query_stack.append(item[key])
                    elif key == "$nor":
                        query_tree_stack.append({
                            "op": not_,
                            "expressions": []
                        })
                        query_stack.append("POP_query_tree_stack")
                        query_stack.append({"$or": item[key]})
                    elif key == "$all":
                        if not isinstance(item[key], list):
                            raise InvalidMQLException(
                                "$all must contain a list.")
                        query_tree_stack.append({
                            "op": and_,
                            "expressions": []
                        })
                        query_stack.append("POP_query_tree_stack")
                        #TODO not sure how an $all to start a query
                        #would function.
                        attr_name = attr_name_stack[-1]
                        for sub_item in item[key]:
                            if isinstance(item[key], dict):
                                query_stack.append({attr_name: sub_item})
                            else:
                                query_stack.append(
                                    {attr_name: {"$eq": sub_item}})

                    elif key == "$elemMatch":
                        attr_name = _get_full_attr_name(attr_name_stack)
                        parent_sub_query_names = _get_full_attr_name(
                            sub_query_name_stack)
                        #trim the parent sub query attrs from
                        #the beginning of the attr_name
                        #note that parent_sub_query_names will always
                        #at least contain RecordClass at the start.
                        sub_query_name = attr_name[len(
                            parent_sub_query_names) + 1:]
                        sub_query_name_stack.append(sub_query_name)
                        query_stack.append("POP_sub_query_name_stack")
                        query_stack.append("POP_query_tree_stack")
                        #query_stack.append(")")
                        query_stack.append(item[key])
                        prop_types = _get_property_types(RecordClass, attr_name)
                        SubClass = prop_types[-1]
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
                                "$elemMatch not applied to subobject.")
                    elif _is_whitelisted(RecordClass,
                                         _get_full_attr_name(
                                             attr_name_stack, key),
                                         whitelist):
                        if len(attr_name_stack) > len(sub_query_name_stack):
                            #nested attribute queries aren't allowed.
                            #this type of search implies an equality
                            #check on an object.
                            raise InvalidMQLException(
                                "Nested attribute queries are not allowed.")
                        #Next couple blocks of code help us find
                        #the first new relationship property
                        #in our attr hierarchy.
                        #TODO - Do this in a more pythonic way.
                        #find the properties that are relationship
                        # properties in our attr hierarchy.
                        full_attr_name = _get_full_attr_name(
                            attr_name_stack, key)
                        prop_types = _get_property_types(
                            RecordClass, full_attr_name)
                        split_full_attr = full_attr_name.split('.')
                        relation_indexes = []
                        for i, prop_type in enumerate(prop_types):
                            if (hasattr(prop_type, "property") and
                                    type(prop_type.property) ==
                                    RelationshipProperty):
                                if (i == len(prop_types) - 1 or not
                                        split_full_attr[i+1][0].isdigit()):
                                    relation_indexes.append(i)
                        #find the properties that are relationships
                        #that already have subqueries in our
                        #attr hierarchy.
                        #psq stands for parent_sub_query
                        psq_attr_name = _get_full_attr_name(
                            sub_query_name_stack)
                        psq_prop_types = _get_property_types(
                            RecordClass, psq_attr_name)
                        psq_split_attr_name = psq_attr_name.split('.')
                        psq_relation_indexes = []
                        for i, prop_type in enumerate(psq_prop_types):
                            if (hasattr(prop_type, "property") and
                                    type(prop_type.property) ==
                                    RelationshipProperty):
                                if (i == len(psq_prop_types) - 1 or not
                                        psq_split_attr_name[i+1][0].isdigit()):
                                    psq_relation_indexes.append(i)
                        if len(psq_relation_indexes) == len(relation_indexes):
                            #There is no new relationship query
                            attr_name_stack.append(key)
                            query_stack.append("POP_attr_name_stack")
                            if isinstance(item[key], dict):
                                query_stack.append(item[key])
                            else:
                                query_stack.append({"$eq": item[key]})
                        elif len(relation_indexes) > len(psq_relation_indexes):
                            #Parse out the next relation sub query
                            #e.g.
                            #full_attr_name =
                            #RClass.prop1.Relation1.prop2.Relation2.p2
                            #sub_query_name_stack =
                            #[RClass, prop1.Relation1]
                            #result:
                            #prop2.Releation2
                            new_relation_index = relation_indexes[len(
                                psq_relation_indexes)]
                            attr_name = ""
                            #get the last relation index from the psq
                            prior_relation_index = 0
                            if len(psq_relation_indexes) > 0:
                                prior_relation_index = psq_relation_indexes[-1]
                            for i in range(prior_relation_index + 1,
                                           new_relation_index + 1):
                                attr_name += split_full_attr[i]
                                if i != new_relation_index:
                                    attr_name += "."
                            attr_name_stack.append(attr_name)
                            query_stack.append("POP_attr_name_stack")
                            #now parse out any remaining property names.
                            #in the above example, this would be p2
                            sub_attr_name = ""
                            for i in range(new_relation_index + 1,
                                           len(split_full_attr)):
                                sub_attr_name += split_full_attr[i]
                                if i != (len(split_full_attr) - 1):
                                    sub_attr_name += "."
                            #below generated $elemMatches will end up
                            #appending the attr_name (that was already
                            #appended to the attr_name_stack) to the
                            #sub_query_name_stack.
                            if (new_relation_index == relation_indexes[-1] and
                                    isinstance(item[key], dict)):
                                if sub_attr_name != "":
                                    #querying a single attribute of this
                                    #relation.
                                    query_stack.append({"$elemMatch": {
                                        sub_attr_name: item[key]}})
                                else:
                                    if not len(item[key].keys() > 0):
                                        #dictionary has no keys, invalid query.
                                        raise InvalidMQLException(
                                            "Attribute " + full_attr_name +
                                            " can't be compared to an empty " +
                                            " dictionary.")
                                    else:
                                        #TODO may also want to check
                                        #for invalid sub_keys. A bad
                                        #key at this point would throw
                                        #an exception we aren't
                                        #catching.
                                        query_tree_stack.append({
                                            "op": and_,
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
                                    #item[key] is not a dict and there
                                    #is no sub_attr, so we're trying to
                                    #equality check a relation.
                                    raise InvalidMQLException(
                                        "Relation " + full_attr_name +
                                        " can't be checked for equality.")
                                else:
                                    #must have a sub_attr, so turn into
                                    #an elemMatch for that sub_attr.
                                    query_stack.append({"$elemMatch": {
                                        sub_attr_name: item[key]}})
                    elif key in set(["$eq", "$neq", "$lt", "$lte", "$gte",
                                     "$gt", "$mod", "$like"]):
                        prop_types = _get_property_types(
                            RecordClass,
                            _get_full_attr_name(attr_name_stack))
                        if (prop_types and
                                hasattr(prop_types[-1], "property") and
                                type(prop_types[-1].property) ==
                                ColumnProperty):
                            target_type = (
                                prop_types[-1].property.columns[0].type)
                            attr = prop_types[-1]
                        else:
                            raise InvalidMQLException(
                                "Relation " + full_attr_name +
                                " can't be checked for equality.")
                        if key == "$lt":
                            expression = attr < _convert_to_alchemy_type(
                                item[key], target_type)
                        elif key == "$lte":
                            expression = attr <= _convert_to_alchemy_type(
                                item[key], target_type)
                        elif key == "$eq":
                            expression = attr == _convert_to_alchemy_type(
                                item[key], target_type)
                        elif key == "$neq":
                            expression = attr != _convert_to_alchemy_type(
                                item[key], target_type)
                        elif key == "$gte":
                            expression = attr >= _convert_to_alchemy_type(
                                item[key], target_type)
                        elif key == "$gt":
                            expression = attr > _convert_to_alchemy_type(
                                item[key], target_type)
                        elif key == "$like":
                            expression = attr.like(_convert_to_alchemy_type(
                                item[key], target_type))
                        elif key == "$in" or key == "$nin":
                            if not isinstance(item[key], list):
                                raise InvalidMQLException(
                                    key + " must contain a list.")
                            converted_list = []
                            for value in item[key]:
                                converted_list.append(
                                    _convert_to_alchemy_type(
                                        value, target_type))
                            expression = attr.in_(converted_list)
                            if key == "$nin":
                                expression = not_(expression)
                        elif key == "$mod":
                            if (isinstance(item[key], list) and
                                    len(item[key]) == 2):
                                try:
                                    divider = int(item[key][0])
                                    result = int(item[key][1])
                                except ValueError:
                                    raise InvalidMQLException(
                                        "Invalid $mod values supplied.")
                                expression = attr.op("%")(divider) == result
                            else:
                                raise InvalidMQLException(
                                    "Invalid $mod values supplied.")
                        query_tree_stack[-1]["expressions"].append(expression)
        if query_tree_stack[-1]["expressions"]:
            query = query.filter(and_(*query_tree_stack[-1]["expressions"]))
        return query


def _get_full_attr_name(attr_name_stack, short_attr_name=None):
    """Join the attr_name_stack to get a full attribute name."""
    attr_name = ""
    is_first = True
    for parent_attr in attr_name_stack:
        if not is_first:
            attr_name += "."
        else:
            is_first = False
        attr_name += parent_attr
    if short_attr_name is not None:
        if not is_first:
            attr_name += "."
        attr_name += short_attr_name
    return attr_name

def _is_whitelisted(RecordClass, attr_name, whitelist):
    """Check if this attr_name is approved to be filtered or sorted."""
    try:
        _get_property_types(RecordClass, attr_name)
    except InvalidMQLException:
        #RecordClass doesn't contain this attr_name,
        #therefor it can't be queried.
        return False
    if whitelist is None:
        return True
    attr_name = str(attr_name)
    if attr_name.startswith(RecordClass.__name__):
        attr_name = attr_name[(len(RecordClass.__name__) + 1):]
        split_attr = attr_name.split(".")
        #parse out any list indexes. List1.0.other becomes Lis1.other
        attr_name = [attr for attr in split_attr if not attr[0].isdigit()]
        attr_name = attr_name.join(".")
        if attr_name in whitelist:
            return True
    return False

def _get_property_types(RecordClass, attr_name):
    """Get info about each attr given a dot notation attr_name."""
    split_attr = attr_name.split(".")
    #We assume the full attr name includes the RecordClass
    #Thus we pop the first name.
    #e.g. RecordClass.prop.subprop becomes prop.subprop
    split_attr.pop(0)
    prop_types = []
    root_type = RecordClass
    prop_types.append(root_type)
    if len(split_attr) > 0:
        for attr_name in split_attr:
            if (hasattr(root_type, "property") and
                    type(root_type.property) == RelationshipProperty):
                if len(attr_name) > 0 and attr_name[0].isdigit():
                    prop_types.append(inspect(root_type).mapper.class_)
                    continue
                else:
                    root_type = inspect(root_type).mapper.class_
            for prop_name in dir(root_type):
                if prop_name == attr_name:
                    prop_type = getattr(root_type, prop_name)
                    root_type = prop_type
                    prop_types.append(prop_type)
                    break
    if (len(split_attr) + 1) != len(prop_types):
        raise InvalidMQLException(
            "The attribute provided does not exist in this class.")
    return prop_types

def _convert_to_alchemy_type(value, alchemy_type):
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
    if type(alchemy_type) in int_types:
        if not isinstance(value, int):
            result = int(value)
        else:
            result = value
    elif type(alchemy_type) in text_types:
        result = str(value)
    elif type(alchemy_type) in bool_types:
        if not isinstance(value, bool):
            if (str(value).lower() == "false" or
                    value == "0" or
                    value == False or
                    value == 0 or
                    value == None):
                result = False
            else:
                result = True
        else:
            result = value
    elif type(alchemy_type) in date_types:
        if not isinstance(value, datetime.date):
            result = datetime.datetime.strptime(value, '%Y-%m-%d').date()
        else:
            result = value
    elif type(alchemy_type) in datetime_types:
        if not isinstance(value, datetime.datetime):
            result = datetime.datetime.strptime(value, '%Y-%m-%d %H:%M:%S')
        else:
            result = value
    elif type(alchemy_type) in float_types:
        if not isinstance(value, float):
            result = float(value)
        else:
            result = value
    elif type(alchemy_type) in time_types:
        if not isinstance(value, datetime.time):
            result = datetime.datetime.strptime(value, '%H:%M:%S').time()
        else:
            result = value
    try:
        return result
    except NameError:
        raise InvalidMQLException("Unable to convert value to alchemy type.")


