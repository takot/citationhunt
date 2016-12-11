import os
import sys
_upper_dir = os.path.abspath(
    os.path.join(os.path.dirname(__file__), '..'))
if _upper_dir not in sys.path:
    sys.path.append(_upper_dir)

import config

def page_ids_to_titles(wpdb, page_ids):
    page_ids = map(str, page_ids)
    result = wpdb.execute_with_retry_s(
        'SELECT page_id, page_title FROM page WHERE ' +
        'page_id IN (' + ','.join(page_ids) + ')')
    return dict(result)

def _expand_categories_once(wpdb, categories):
    """Expand a set of categories once.

    Returns a tuple (page ids, subcategory ids) of all page ids and
    subcategories in any of the given categories.
    """

    page_ids = set()
    subcategory_ids = set()
    expansion = wpdb.execute_with_retry_s(
        'SELECT cl_from, cl_to, cl_type FROM categorylinks WHERE ' +
        'cl_to IN (' + ','.join(['%s'] * len(categories)) + ')', *categories)

    for page_id, category, type in expansion:
        if type == 'page':
            page_ids.add(page_id)
        elif type == 'subcat':
            subcategory_ids.add(page_id)
    return page_ids, subcategory_ids

def _expand_category(wpdb, category):
    """Expands a category recursively.

    Returns a set of page ids for the leaf pages in the category.
    """

    categories = set([category])
    result = set()
    while True:
        page_ids, subcategory_ids = _expand_categories_once(wpdb, categories)
        result |= page_ids
        if not subcategory_ids:
            break
        # need to convert the page ids of subcategories into page
        # titles so we can query recursively
        categories = set(page_ids_to_titles(wpdb, subcategory_ids).values())
    return result

def expand_category_to_page_ids(wpdb, category):
    return _expand_category(wpdb, category)

def expand_category_to_subcategories(wpdb, category):
    _, subcategory_ids = _expand_categories_once(wpdb, [category])
    return set(page_ids_to_titles(wpdb, subcategory_ids).values())
