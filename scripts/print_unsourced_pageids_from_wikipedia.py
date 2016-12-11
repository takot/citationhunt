#!/usr/bin/env python

import os
import sys
_upper_dir = os.path.abspath(
    os.path.join(os.path.dirname(__file__), '..'))
if _upper_dir not in sys.path:
    sys.path.append(_upper_dir)

import chdb
import config
from wplib import categories

def print_unsourced_ids_from_wikipedia():
    cfg = config.get_localized_config()
    db = chdb.init_wp_replica_db()
    print '\n'.join(map(str, categories.expand_category_to_page_ids(
        db, cfg.citation_needed_category)))

if __name__ == '__main__':
    print_unsourced_ids_from_wikipedia()
