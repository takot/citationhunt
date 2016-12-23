import MySQLdb
import MySQLdb.cursors

import config
import warnings
import os.path as op
import contextlib
import functools
import threading

ch_my_cnf = op.join(op.dirname(op.realpath(__file__)), 'ch.my.cnf')
wp_my_cnf = op.join(op.dirname(op.realpath(__file__)), 'wp.my.cnf')

class ConnectionPool(object):
    '''
    A pool for MySQLdb Connection objects, identified by the config file
    used to initialize them.
    '''

    def __init__(self):
        self._lock = threading.Lock()
        self._free_connections = {}

    def return_connection(self, conn):
        '''Return a connection to the pool'''
        with self._lock:
            # We assume the server will start disconnecting old connections, at
            # which point they'll be garbage-collected using swap_connection
            # later, so don't bother bounding the number of connections
            connections = self._free_connections[conn._config_file]
            connections.append(conn)

    def swap_connection(self, conn):
        '''Swap a bad connection for a new connection'''
        config_file, initializer = conn._config_file, conn._initializer
        with self._lock:
            assert conn not in self._free_connections[config_file]
        conn.close = conn.really_close
        return self.get_connection(config_file, initializer)

    # FIXME: This should probably return a cursor instead of a connection.
    # If we return a stale connection and it gets used as a context manager,
    # even if the RetryingCursor does the right thing and refreshes the
    # connection, the stale connection's __exit__ will get called and raise an
    # exception just the same.
    def get_connection(self, config_file, initializer = None):
        '''Get a connection from the pool.

        The `config_file` will be sourced to initialize the connection.
        Additionally, the `initializer` callback will be called before
        returning.
        '''
        with self._lock:
            if not self._free_connections.setdefault(config_file, []):
                conn = MySQLdb.connect(
                    charset = 'utf8mb4', read_default_file = config_file,
                    cursorclass = RetryingCursor)
                conn.really_close = conn.close
                conn.close = functools.partial(self.return_connection, conn)
                conn._config_file = config_file
                self._free_connections[config_file].append(conn)
            conn = self._free_connections[config_file].pop()

        # keep track of the initializer for swapping
        conn._initializer = initializer
        if callable(conn._initializer):
            conn._initializer(conn)
        return conn
_connection_pool = ConnectionPool()

class RetryingCursor(MySQLdb.cursors.Cursor):
    def _with_retry(self, mth, query, *args):
        max_retries = 5
        for retry in range(max_retries):
            try:
                mth(query, *args)
            except MySQLdb.OperationalError:
                if retry == max_retries - 1:
                    raise
                else:
                    connection = _connection_pool.swap_connection(
                        self.connection)
                    super(RetryingCursor, self).__init__(connection)
            else:
                break

    def execute(self, query, *args):
        return self._with_retry(
            super(RetryingCursor, self).execute, query, *args)

    def executemany(self, query, *args):
        return self._with_retry(
            super(RetryingCursor, self).executemany, query, *args)

    def close(self):
        # FIXME ConnectionPool should handle the same connection being returned
        # more than once.
        _connection_pool.return_connection(self.connection)
        super(RetryingCursor, self).close()

@contextlib.contextmanager
def ignore_warnings():
    warnings.filterwarnings('ignore', category = MySQLdb.Warning)
    yield
    warnings.resetwarnings()

def _make_tools_labs_dbname(db, database, lang_code):
    cursor = db.cursor()
    cursor.execute("SELECT SUBSTRING_INDEX(USER(), '@', 1)")
    user = cursor.fetchone()[0]
    return '%s__%s_%s' % (user, database, lang_code)

def _ensure_database(database, lang_code):
    def _ensure_database_with_db(db):
        with db as cursor:
            dbname = _make_tools_labs_dbname(db, database, lang_code)
            with ignore_warnings():
                cursor.execute('SET SESSION sql_mode = ""')
                cursor.execute(
                    'CREATE DATABASE IF NOT EXISTS '
                    '%s CHARACTER SET utf8mb4' % dbname)
            cursor.execute('USE %s' % dbname)
    return _ensure_database_with_db

def init_db(lang_code):
    return _connection_pool.get_connection(
        ch_my_cnf, _ensure_database('citationhunt', lang_code))

def init_scratch_db():
    cfg = config.get_localized_config()
    return _connection_pool.get_connection(
        ch_my_cnf, _ensure_database('scratch', cfg.lang_code))

def init_stats_db():
    def initialize(db):
        _ensure_database('stats', 'global')(db)
        with db as cursor, ignore_warnings():
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS requests (
                ts DATETIME, lang_code VARCHAR(4), snippet_id VARCHAR(128),
                category_id VARCHAR(128), url VARCHAR(768), prefetch BOOLEAN,
                status_code INTEGER, referrer VARCHAR(128))
                ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            ''')
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS fixed (
                clicked_ts DATETIME, snippet_id VARCHAR(128) UNIQUE,
                lang_code VARCHAR(4))
                ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            ''')
            # Create per-language views for convenience
            for lang_code in config.LANG_CODES_TO_LANG_NAMES:
                cursor.execute('''
                    CREATE OR REPLACE VIEW requests_''' + lang_code +
                    ''' AS SELECT * FROM requests WHERE lang_code = %s
                ''', (lang_code,))
                cursor.execute('''
                    CREATE OR REPLACE VIEW fixed_''' + lang_code +
                    ''' AS SELECT * FROM fixed WHERE lang_code = %s
                ''', (lang_code,))
        return db
    return _connection_pool.get_connection(ch_my_cnf, initialize)

def init_wp_replica_db():
    cfg = config.get_localized_config()
    def initialize(db):
        with db as cursor:
            cursor.execute('USE ' + cfg.database)
    return _connection_pool.get_connection(wp_my_cnf, initialize)

def init_projectindex_db():
    def initialize(db):
        with db as cursor:
            cursor.execute('USE s52475__wpx_p')
    return _connection_pool.get_connection(ch_my_cnf, initialize)

def reset_scratch_db():
    cfg = config.get_localized_config()
    db = init_db(cfg.lang_code)
    with db as cursor:
        dbname = _make_tools_labs_dbname(db, 'scratch', cfg.lang_code)
        with ignore_warnings():
            cursor.execute('DROP DATABASE IF EXISTS ' + dbname)
        cursor.execute('CREATE DATABASE %s CHARACTER SET utf8mb4' % dbname)
        cursor.execute('USE ' + dbname)
    create_tables(db)
    return db

def install_scratch_db():
    cfg = config.get_localized_config()
    db = init_db(cfg.lang_code)
    # ensure citationhunt is populated with tables
    create_tables(db)

    chname = _make_tools_labs_dbname(db, 'citationhunt', cfg.lang_code)
    scname = _make_tools_labs_dbname(db, 'scratch', cfg.lang_code)
    with db as cursor:
        # generate a sql query that will atomically swap tables in
        # 'citationhunt' and 'scratch'. Modified from:
        # http://blog.shlomoid.com/2010/02/emulating-missing-rename-database.html
        cursor.execute('''
            SELECT CONCAT('RENAME TABLE ',
            GROUP_CONCAT('%s.', table_name,
            ' TO ', table_schema, '.old_', table_name, ', ',
            table_schema, '.', table_name, ' TO ', '%s.', table_name),';')
            FROM information_schema.TABLES WHERE table_schema = '%s'
            GROUP BY table_schema;
        ''' % (chname, chname, scname))

        rename_stmt = cursor.fetchone()[0]
        cursor.execute(rename_stmt)
        cursor.execute('DROP DATABASE ' + scname)

def create_tables(db):
    cfg = config.get_localized_config()
    with db as cursor, ignore_warnings():
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS categories (id VARCHAR(128) PRIMARY KEY,
            title VARCHAR(255)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        ''')
        cursor.execute('''
            INSERT IGNORE INTO categories VALUES("unassigned", "unassigned")
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS articles (page_id INT(8) UNSIGNED
            PRIMARY KEY, url VARCHAR(512), title VARCHAR(512))
            ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS articles_categories (
            article_id INT(8) UNSIGNED, category_id VARCHAR(128),
            FOREIGN KEY(article_id) REFERENCES articles(page_id)
            ON DELETE CASCADE,
            FOREIGN KEY(category_id) REFERENCES categories(id)
            ON DELETE CASCADE) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS category_article_count (
            category_id VARCHAR(128), article_count INT(8) UNSIGNED,
            FOREIGN KEY(category_id) REFERENCES categories(id)
            ON DELETE CASCADE) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS snippets (id VARCHAR(128) PRIMARY KEY,
            snippet VARCHAR(%s), section VARCHAR(768), article_id INT(8)
            UNSIGNED, FOREIGN KEY(article_id) REFERENCES articles(page_id)
            ON DELETE CASCADE) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        ''', (cfg.snippet_max_size * 2,))
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS snippets_links (prev VARCHAR(128),
            next VARCHAR(128), cat_id VARCHAR(128),
            FOREIGN KEY(prev) REFERENCES snippets(id) ON DELETE CASCADE,
            FOREIGN KEY(next) REFERENCES snippets(id) ON DELETE CASCADE,
            FOREIGN KEY(cat_id) REFERENCES categories(id) ON DELETE CASCADE)
            ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        ''')
