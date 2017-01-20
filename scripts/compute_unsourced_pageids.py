#!/usr/bin/env python

import os
import sys
_upper_dir = os.path.abspath(
    os.path.join(os.path.dirname(__file__), '..'))
if _upper_dir not in sys.path:
    sys.path.append(_upper_dir)

import chdb
import config
from utils import *

import itertools

log = Logger()

def reuse_unmodified_pages(cfg, wpdb):
    all_unmodified_page_ids = set()
    live_db = chdb.init_db(cfg.lang_code)

    # grab the pages and revisions that already exist in the live database
    try:
        rev_and_page_id = live_db.execute_with_retry_s(
            'SELECT page_id, rev_id FROM articles')
    except:
        log.info('failed to query the live database (is there one?), '
            "won't reuse any existing data")
        return all_unmodified_page_ids

    # precompute the queries we'll use to move unmodified data from the live to
    # the scratch database. this assumes both databases exist in the same host.
    scratch_db_name = chdb.make_dbname(
        live_db, chdb.DBNAME_SCRATCH, cfg.lang_code)
    live_db_name = chdb.make_dbname(
        live_db, chdb.DBNAME_LIVE, cfg.lang_code)
    copy_unmodified_articles_query = (
        'INSERT INTO {scratch_db_name}.articles '
        'SELECT * FROM {live_db_name}.articles '
        'WHERE {live_db_name}.articles.page_id IN %s'.format(
            scratch_db_name = scratch_db_name,
            live_db_name = live_db_name))
    copy_unmodified_snippets_query = (
        'INSERT INTO {scratch_db_name}.snippets '
        'SELECT * FROM {live_db_name}.snippets '
        'WHERE {live_db_name}.snippets.article_id IN %s'.format(
            scratch_db_name = scratch_db_name,
            live_db_name = live_db_name))

    for c in ichunk(rev_and_page_id, 4096):
        page_ids, rev_ids = itertools.izip(*c)
        unmodified_page_ids = [
            row[0] for row in wpdb.execute_with_retry_s(
                'SELECT page_id FROM page '
                'WHERE page_id IN %s AND page_latest IN %s',
                page_ids, rev_ids)]
        live_db.execute_with_retry_s(
            copy_unmodified_articles_query, unmodified_page_ids)
        live_db.execute_with_retry_s(
            copy_unmodified_snippets_query, unmodified_page_ids)
        all_unmodified_page_ids |= set(unmodified_page_ids)
    log.info('reusing %d unmodified pages' % len(all_unmodified_page_ids))
    return all_unmodified_page_ids

def compute_unsourced_pageids():
    chdb.reset_scratch_db()
    cfg = config.get_localized_config()
    wpdb = chdb.init_wp_replica_db()
    unmodified_page_ids = reuse_unmodified_pages(cfg, wpdb)
    categories = set([cfg.citation_needed_category])
    while True:
        subcategories = set()
        page_id_and_type = wpdb.execute_with_retry_s(
            'SELECT cl_from, cl_type FROM categorylinks WHERE (' +
            ' OR '.join(['cl_to = %s'] * len(categories)) + ')', *categories)
        for page_id, type in page_id_and_type:
            if type == 'page' and int(page_id) not in unmodified_page_ids:
                print page_id
            elif type == 'subcat':
                subcategories.add(page_id)
        if not subcategories:
            break

        # need to convert the page ids of subcategories into page
        # titles so we can query recursively
        categories = set(
            row[0] for row in wpdb.execute_with_retry_s(
                'SELECT page_title FROM page WHERE (' +
                ' OR '.join(['page_id = %s'] * len(subcategories)) + ')',
                *subcategories))

if __name__ == '__main__':
    compute_unsourced_pageids()
