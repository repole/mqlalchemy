## -*- coding: utf-8 -*-\
"""
    mqlalchemy.utils
    ~~~~~~~~~~~~~~~~~~~~~

    Utility functions for MQLAlchemy.

"""
# :copyright: (c) 2020 by Nicholas Repole and contributors.
#             See AUTHORS for more details.
# :license: MIT - See LICENSE for more details.


def dummy_gettext(string, **variables):
    """Simple gettext stand in for when none is provided.

    :param string: Input text with optional variable placeholders.
    :param variables: Key word args used to populate any placeholder
        variables in the provided string.

    """
    return string % variables
