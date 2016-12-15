import chdb
import config
from utils import *
from common import *
import snippet_parser

import wikitools

import time
import itertools
import cStringIO as StringIO
import requests
import collections
import urllib
import urlparse

# the markup we're going to use for [citation needed] and <ref> tags,
# pre-marked as safe for jinja.
SUPERSCRIPT_HTML = '<sup class="superscript">[%s]</sup>'
SUPERSCRIPT_MARKUP = flask.Markup(SUPERSCRIPT_HTML)
CITATION_NEEDED_MARKUP = flask.Markup(SUPERSCRIPT_HTML)

Category = collections.namedtuple('Category', ['id', 'title'])
CATEGORY_ALL = Category('all', '')

# A class wrapping database access functions so they're easier to
# mock when testing.
class Database(object):
    @staticmethod
    def query_category_by_id(lang_code, cat_id):
        cursor = get_db(lang_code).cursor()
        cursor.execute('''
            SELECT id, title FROM categories WHERE id = %s
        ''', (cat_id,))
        return cursor.fetchone()

    @staticmethod
    def query_random_page(lang_code):
        cursor = get_db(lang_code).cursor()
        ret = None
        # For small datasets, the probability of getting an empty result in a
        # query is non-negligible, so retry a bunch of times as needed.
        p = '1e-4' if not flask.current_app.debug else '1e-2'
        for retry in range(100):
            cursor.execute(
                'SELECT page_id FROM articles WHERE RAND() < %s LIMIT 1;',
                (p,))
            ret = cursor.fetchone()
            if ret is not None: break
        return ret

    @staticmethod
    def query_pages_in_category(lang_code, category):
        assert category != CATEGORY_ALL
        cursor = get_db(lang_code).cursor()
        cursor.execute('''
            SELECT article_id FROM articles_categories
            WHERE articles_categories.category_id = %s;''', (category.id,))
        return cursor.fetchall()

    @staticmethod
    def search_category(lang_code, needle, max_results):
        cursor = get_db(lang_code).cursor()
        needle = '%' + needle + '%'
        cursor.execute('''
            SELECT id, title FROM categories WHERE title LIKE %s
            LIMIT %s''', (needle, max_results))
        return [{'id': row[0], 'title': row[1]} for row in cursor]

def make_snippet_id(page_id, snippet):
    return '%sg%08x' % (mkid(snippet)[:3], page_id)

def parse_snippet_id(id):
    try:
        snippet, page = id.split('g')
        pageid = int(page, 16)
        return pageid, snippet
    except:
        # Invalid snippet
        return (None, None)

class Snippet(object):
    def __init__(self, page, section, snippet):
        self.page = page
        self.section = section
        self.snippet = snippet
        self.id = make_snippet_id(page.pageid, snippet)

# An adapter that lets us use requests for wikitools until it doesn't grow
# native support. This allows us to have persistent connections.
class WikitoolsRequestsAdapter(object):
    def __init__(self):
        self.session = requests.Session()

    def open(self, request):
        headers = dict(request.headers)
        headers.pop('Content-length') # Let requests compute this
        response = self.session.get(
            request.get_full_url() + '?' + request.get_data(),
            headers = headers)
        return urllib.addinfourl(
            StringIO.StringIO(response.text), request.headers,
            request.get_full_url(), response.status_code)
opener = WikitoolsRequestsAdapter()

APIRequest = wikitools.api.APIRequest
class RequestsAPIRequest(wikitools.api.APIRequest):
    def __init__(self, *args, **kwds):
        APIRequest.__init__(self, *args, **kwds)
        self.opener = opener
wikitools.api.APIRequest = RequestsAPIRequest

# FIXME Move this to snippet_parser
def to_html(wikipedia, text):
    params = {
        'action': 'parse',
        'format': 'json',
        'text': text
    }
    request = wikitools.APIRequest(wikipedia, params)
    # FIXME Sometimes the request fails because the text is too long;
    # in that case, the API response is HTML, not JSON, which raises
    # an exception when wikitools tries to parse it.
    #
    # Normally this would cause wikitools to happily retry forever
    # (https://github.com/alexz-enwp/wikitools/blob/b71481796c350/wikitools/api.py#L304),
    # which is a bug, but due to our use of a custom opener, wikitools'
    # handling of the exception raises its own exception: the object returned
    # by our opener doesnt support seek().
    #
    # We use that interesting coincidence to catch the exception and move
    # on, bypassing wikitools' faulty retry, but this is obviously a terrible
    # "solution".
    try:
        ret = request.query()['parse']['text']['*']
    except:
        return ''

    # Links are always relative so they end up broken in the UI. We could make
    # them absolute, but let's just remove them (by replacing with <span>) since
    # we don't actually need them.
    import xml.etree.cElementTree as ET
    ret = '<div>' + ret + '</div>'
    tree = ET.fromstring(e(ret))
    for parent_of_a in tree.findall('.//a/..'):
        for i, tag in enumerate(parent_of_a):
            if tag.tag == 'a' and tag.text:
                repl = ET.Element('span')
                repl.extend(list(tag))
                if tag.text:
                    repl.text = tag.text
                if tag.tail:
                    repl.tail = tag.tail + ' '
                else:
                    repl.tail = ' '
                parent_of_a.insert(i+1, repl)
                parent_of_a.remove(tag)
    return d(ET.tostring(tree))

_cache = {}
def cache_with_timeout(ttl):
    def decorator(fn):
        @functools.wraps(fn)
        def wrapper(*args, **kwds):
            key = fn.__name__, args
            if key in _cache:
                ts, result = _cache[key]
                if time.time() - ts < ttl:
                    return result
            _cache[key] = (time.time(), fn(*args, **kwds))
            return _cache[key][1]
        return wrapper
    return decorator

@cache_with_timeout(30 * 60 * 60)
def _parse_page(pageid, wikipedia, parser, cfg):
    page = wikitools.Page(wikipedia, pageid = pageid)
    with log_time('fetch wikitext for pageid %d' % pageid):
        wikitext = page.getWikiText()
    with log_time('extract snippets'):
        # FIXME if cfg.extract_section
        if cfg.html_snippet:
            parse_result = parser.extract_sections(wikitext)
        else:
            parse_result = parser.extract_snippets(
                wikitext, cfg.snippet_min_size, cfg.snippet_max_size)
    ret = []
    for section, snippets in parse_result:
        for sni in snippets:
            if cfg.html_snippet:
                sni = to_html(wikipedia, sni)
            if not (cfg.snippet_min_size < len(sni) < cfg.snippet_max_size):
                continue
            ret.append(Snippet(page, section, sni))
    return sorted(ret, key = lambda s: s.id)

class SnippetManager(object):
    def __init__(self, cfg):
        self.cfg = cfg
        self.wikipedia = wikitools.wiki.Wiki(
            'https://' + cfg.wikipedia_domain + '/w/api.php')
        self.wikipedia.setUserAgent(
            'citationhunt (https://tools.wmflabs.org/citationhunt)')
        self.parser = snippet_parser.create_snippet_parser(cfg)

    def _parse_page(self, pageid):
        return _parse_page(pageid, self.wikipedia, self.parser, self.cfg)

    def next_snippet_in_page(self, pageid, current = None):
        snippets = self._parse_page(pageid)
        if snippets and current is None:
            return snippets[0]
        for snippet in snippets:
            if snippet.id > current:
                return snippet
        return None

    def next_snippet_in_category(self, category, current = None):
        if category == CATEGORY_ALL:
            return self.random_snippet()

        if current is not None:
            pageid, _ = parse_snippet_id(current)
            # Note that this method should only get called after whe know
            # the current id is valid, so we can afford the sanity check below
            assert pageid is not None, 'Invalid snippet id?!'

            # Try the next in the current page
            next = self.next_snippet_in_page(pageid, current)
            if next is not None:
                return next

        # No luck, use the next page in the category, wrapping around
        # to the first if we're done. We may end up returning the same
        # snippet due to this, but that should be extremely rare.
        pageids_in_category = sorted(
            itertools.chain(*Database.query_pages_in_category(
            self.cfg.lang_code, category)))
        if current is not None:
            try:
                current_idx = pageids_in_category.index(pageid)
            except ValueError:
                # The snippet didn't belong to the category to begin with!
                return None
        else:
            import random
            current_idx = random.randint(0, len(pageids_in_category)+1)

        pageids_in_category = (
            pageids_in_category[current_idx+1:] +
            pageids_in_category[:current_idx+1])

        for pid in pageids_in_category:
            next = self.next_snippet_in_page(pid)
            if next is not None: return next
        return None

    def random_snippet(self):
        # In theory, it's possible that parsing the random page yields no
        # snippets (if, for example, there was only one snippet in the page and
        # it's already been fixed). We expect this to be rare, but retry a few
        # times.
        for retry in range(5):
            pageid = Database.query_random_page(self.cfg.lang_code)
            assert pageid and len(pageid) == 1
            pageid = pageid[0]
            snippet = self.next_snippet_in_page(pageid)
            if snippet is not None:
                break
        return snippet

    def get_snippet_by_id(self, id):
        pageid, _ = parse_snippet_id(id)
        if not pageid:
            return None
        snippets = self._parse_page(pageid)
        for snippet in snippets:
            if snippet.id == id:
                # TODO it's a bit weird to return this tuple here
                title = d(snippet.page.title)
                return (
                    snippet.snippet,
                    snippet.section,
                    'https://' + self.cfg.wikipedia_domain + '/wiki/' + title,
                    title
                )
        return None

_snippet_mgrs = {}
def get_snippet_manager(cfg):
    if cfg.lang_code not in _snippet_mgrs:
        _snippet_mgrs[cfg.lang_code] = SnippetManager(cfg)
    return _snippet_mgrs[cfg.lang_code]

def get_category_by_id(lang_code, cat_id):
    if cat_id == CATEGORY_ALL.id:
        return CATEGORY_ALL
    c = Database.query_category_by_id(lang_code, cat_id)
    return Category(*c) if c is not None else None

def select_next_id(snippet_mgr, curr_id, cat = CATEGORY_ALL):
    for i in range(3): # super paranoid :)
        next_snippet = snippet_mgr.next_snippet_in_category(cat, curr_id)
        if next_snippet is None:
            return None
        if next_snippet.id != curr_id:
            return next_snippet.id
    return curr_id

def should_autofocus_category_filter(cat, request):
    return cat is CATEGORY_ALL and not request.MOBILE

def section_name_to_anchor(section):
    # See Sanitizer::escapeId
    # https://doc.wikimedia.org/mediawiki-core/master/php/html/classSanitizer.html#ae091dfff62f13c9c1e0d2e503b0cab49
    section = section.replace(' ', '_')
    # urllib.quote interacts really weirdly with unicode in Python2:
    # https://bugs.python.org/issue23885
    section = urllib.quote(e(section), safe = e(''))
    section = section.replace('%3A', ':')
    section = section.replace('%', '.')
    return section

@validate_lang_code
def citation_hunt(lang_code):
    id = flask.request.args.get('id')
    cat = flask.request.args.get('cat')
    cfg = config.get_localized_config(lang_code)
    snippet_mgr = get_snippet_manager(cfg)

    lang_dir = cfg.lang_dir
    if flask.current_app.debug:
        lang_dir = flask.request.args.get('dir', lang_dir)

    if cat is not None:
        cat = get_category_by_id(lang_code, cat)
        if cat is None:
            # invalid category, normalize to "all" and try again by id
            cat = CATEGORY_ALL
            return flask.redirect(
                flask.url_for('citation_hunt',
                    lang_code = lang_code, id = id, cat = cat.id))
    else:
        cat = CATEGORY_ALL

    if id is not None:
        sinfo = snippet_mgr.get_snippet_by_id(id)
        if sinfo is None:
            # invalid id
            flask.request.cfg = cfg
            flask.abort(404)
        snippet, section, aurl, atitle = sinfo
        if cfg.html_snippet:
            snippet = flask.Markup(snippet)
        next_snippet_id = select_next_id(snippet_mgr, id, cat)
        if next_snippet_id is None:
            # the snippet doesn't belong to the category!
            assert cat is not CATEGORY_ALL
            return flask.redirect(
                flask.url_for('citation_hunt',
                    id = id, cat = CATEGORY_ALL.id,
                    lang_code = lang_code))
        autofocus = should_autofocus_category_filter(cat, flask.request)
        article_url_path = urllib.quote(
            e(urlparse.urlparse(aurl).path.lstrip('/')))
        return flask.render_template('index.html',
            snippet = snippet, section = section_name_to_anchor(section),
            article_url = aurl, article_url_path = article_url_path,
            article_title = atitle, current_category = cat,
            next_snippet_id = next_snippet_id,
            cn_marker = snippet_parser.CITATION_NEEDED_MARKER,
            cn_html = CITATION_NEEDED_MARKUP,
            ref_marker = snippet_parser.REF_MARKER,
            ref_html = SUPERSCRIPT_MARKUP,
            config = cfg,
            lang_dir = lang_dir,
            category_filter_autofocus = autofocus)

    snippet = snippet_mgr.next_snippet_in_category(cat)
    if snippet is None:
        # This should be rare, but could happen in a category with few pages
        flask.request.cfg = cfg
        flask.abort(404)
    return flask.redirect(
        flask.url_for('citation_hunt',
            id = snippet.id, cat = cat.id, lang_code = lang_code))

@validate_lang_code
def search_category(lang_code):
    return flask.jsonify(
        results=Database.search_category(
            lang_code, flask.request.args.get('q'), max_results = 400))
