## -*- coding: utf-8 -*-\
"""
    mqlalchemy._compat
    ~~~~~

    Python2/Python3 support helper library.

    :copyright: (c) 2015 by Nicholas Repole.
    :license: BSD - See LICENSE.md for more details.
"""

import sys
if sys.version_info[0] == 2:
    bytes = str
    str = unicode
else:
    str = str
    bytes = bytes
