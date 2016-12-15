#!/usr/bin/env python

'''
Assign categories to the pages in the CitationHunt database.

Usage:
    assign_categories.py [--mysql_config=<FILE>]

Options:
    --mysql_config=<FILE>  MySQL config file [default: ./ch.my.cnf].
'''

from __future__ import unicode_literals

import os
import sys
_upper_dir = os.path.abspath(
    os.path.join(os.path.dirname(__file__), '..'))
if _upper_dir not in sys.path:
    sys.path.append(_upper_dir)

import config
import chdb as chdb_
from utils import *
from wplib import categories

import docopt

import cProfile
import itertools as it
import re
import collections
import pstats
import time

log = Logger()

def ichunk(iterable, chunk_size):
    it0 = iter(iterable)
    while True:
        it1, it2 = it.tee(it.islice(it0, chunk_size))
        next(it2)  # raises StopIteration if it0 is exhausted
        yield it1

class CategoryName(unicode):
    '''
    The canonical format for categories, which is the one we'll use
    in the CitationHunt database: no Category: prefix and spaces instead
    of underscores.
    '''
    def __new__(klass, ustr):
        assert isinstance(ustr, unicode)
        assert not ustr.startswith('Category:'), ustr
        assert '_' not in ustr, ustr
        return super(CategoryName, klass).__new__(klass, ustr)

    @staticmethod
    def from_wp_page(ustr):
        ustr = d(ustr)
        if ustr.startswith('Category:'):
            ustr = ustr[len('Category:'):]
        assert ' ' not in ustr, ustr
        return CategoryName(ustr.replace('_', ' '))

    @staticmethod
    def from_wp_categorylinks(ustr):
        ustr = d(ustr)
        if ustr.startswith('Category:'):
            ustr = ustr[len('Category:'):]
        return CategoryName(ustr.replace('_', ' '))

    @staticmethod
    def from_tl_projectindex(ustr):
        ustr = d(ustr)
        if ustr.startswith('Wikipedia:'):
            ustr = ustr[len('Wikipedia:'):]
        return CategoryName(ustr.replace('_', ' '))

def category_name_to_id(catname):
    return mkid(catname)

def load_categories_for_pages(wpcursor, pageids):
    wpcursor.execute('''
        SELECT cl_to, cl_from FROM categorylinks WHERE cl_from IN %s''',
        (tuple(pageids),))
    return ((CategoryName.from_wp_categorylinks(row[0]), row[1])
            for row in wpcursor)

def load_projectindex(cfg):
    if not running_in_tools_labs() or cfg.lang_code != 'en':
        return []
    tldb = chdb_.init_projectindex_db()
    tlcursor = tldb.cursor()

    # We use a special table on Tools Labs to map page IDs to projects,
    # which will hopefully be more broadly available soon
    # (https://phabricator.wikimedia.org/T131578)
    query = """
    SELECT project_title, page_id
    FROM enwiki_index
    JOIN enwiki_page ON index_page = page_id
    JOIN enwiki_project ON index_project = project_id
    WHERE page_ns = 0 AND page_is_redirect = 0
    """
    tlcursor.execute(query)

    ret = [(CategoryName.from_tl_projectindex(r[0]), r[1]) for r in tlcursor]
    log.info('loaded %d entries from projectinfo (%s...)' % \
        (len(ret), ret[0][0]))
    return ret

def category_is_usable(cfg, catname, hidden_categories):
    assert isinstance(catname, CategoryName)
    if catname in hidden_categories:
        return False
    for regexp in cfg.category_name_regexps_blacklist:
        if re.search(regexp, catname):
            return False
    return True

def update_citationhunt_db(chdb, category_name_id_and_page_ids):
    def insert(cursor, chunk):
        cursor.executemany('''
            INSERT IGNORE INTO articles VALUES (%s)''',
            ((pageid,) for pageid in it.chain(
                *(pageids for _, _, pageids in chunk))))
        cursor.executemany('''
            INSERT IGNORE INTO categories VALUES (%s, %s)
        ''', ((category_id, category_name)
            for category_name, category_id, _ in chunk))
        cursor.executemany('''
            INSERT INTO articles_categories VALUES (%s, %s)
        ''', ((pageid, catid)
            for _, catid, pageids in chunk for pageid in pageids))

    for c in ichunk(category_name_id_and_page_ids, 4096):
        # We're trying to add the same pages multiple times
        with chdb_.ignore_warnings():
            chdb.execute_with_retry(insert, list(c))

def assign_categories(mysql_default_cnf):
    cfg = config.get_localized_config()
    profiler = cProfile.Profile()
    if cfg.profile:
        profiler.enable()
    start = time.time()

    chdb = chdb_.reset_scratch_db()
    wpdb = chdb_.init_wp_replica_db()

    unsourced_pageids = categories.expand_category_to_page_ids(
        wpdb, cfg.citation_needed_category)
    log.info('loaded %d unsourced page ids' % len(unsourced_pageids))

    # Load a list of (wikiproject, page ids), if applicable
    projectindex = load_projectindex(cfg)

    hidden_categories = set(
        CategoryName.from_wp_page(c) for c in
        categories.expand_category_to_subcategories(
            wpdb, cfg.hidden_category))
    log.info('loaded %d hidden categories (%s...)' % \
        (len(hidden_categories), next(iter(hidden_categories))))

    # Load all usable categories into a dict category -> [page ids]
    category_to_page_ids = {}
    for c, p in projectindex:
        if p in unsourced_pageids:
            category_to_page_ids.setdefault(c, []).append(p)
    for c in ichunk(unsourced_pageids, 10000):
        for c, p in wpdb.execute_with_retry(load_categories_for_pages, c):
            if category_is_usable(cfg, c, hidden_categories):
                category_to_page_ids.setdefault(c, []).append(p)

    # Keep only the categories with at least a few unsourced pages
    category_name_id_and_page_ids = [
        (unicode(category), category_name_to_id(category), page_ids)
        for category, page_ids in category_to_page_ids.iteritems()
        if len(category_to_page_ids[category]) >= 3
    ]
    log.info('finished with %d categories' % len(category_name_id_and_page_ids))

    update_citationhunt_db(chdb, category_name_id_and_page_ids)
    wpdb.close()
    chdb.close()
    log.info('all done in %d seconds.' % (time.time() - start))

    if cfg.profile:
        profiler.disable()
        pstats.Stats(profiler).sort_stats('cumulative').print_stats(
            30, 'assign_categories.py')
    return 0

if __name__ == '__main__':
    args = docopt.docopt(__doc__)
    mysql_default_cnf = args['--mysql_config']
    ret = assign_categories(mysql_default_cnf)
    sys.exit(ret)
