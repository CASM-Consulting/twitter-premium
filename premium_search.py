import codecs
import json
import os
from collections import Counter, defaultdict
from searchtweets import ResultStream, gen_rule_payload, load_credentials, collect_results

SANDBOX = False

# Key conf
CRED_FILE_PATH = './.twitter_keys.yaml'
CRED_YAML_SEARCH_KEY = 'search_tweets_api_fullarchive'
CRED_YAML_COUNT_KEY = 'count_tweets_api_fullarchive'
ACC_TYPE = 'premium'

# env_overwrite=False, otherwise by default env variables will be used
# if they are set, regardless of config.
premium_search_args = load_credentials(filename=CRED_FILE_PATH, account_type=ACC_TYPE,
                                       yaml_key=CRED_YAML_SEARCH_KEY, env_overwrite=False)

if SANDBOX:
    POWERTRACK_QUERY_LIMIT = 256
    MAX_PAGES = 1  # Debug
    MAX_RESULTS = 100  # Debug
    RESULTS_PER_CALL = 100  # Sandbox limit
else:
    POWERTRACK_QUERY_LIMIT = 1024
    MAX_PAGES = None
    MAX_RESULTS = None
    RESULTS_PER_CALL = 500

    # Count cred args, only out of sandbox
    premium_count_args = load_credentials(filename=CRED_FILE_PATH, account_type=ACC_TYPE,
                                          yaml_key=CRED_YAML_COUNT_KEY, env_overwrite=False)


def _search(power_track_query, to_date=None, from_date=None, results_per_call=None, max_results=None, max_pages=None):
    """
    :param power_track_query: PowerTrack rule
    :param to_date: "YYYY-mm-DD HH:MM" format (can exclude time)
    :param from_date: "YYYY-mm-DD HH:MM" format (can exclude time)
    :param results_per_call: number of tweets to request per call, sandbox limit is 100
    :param max_results: stop requesting data when we hit to this number of obtained tweets
    :param max_pages: max number of API calls for this session

    :return: generator over tweet objects
    """
    # Create search rule
    rule = gen_rule_payload(
        pt_rule=power_track_query,
        to_date=to_date,
        from_date=from_date,
        results_per_call=results_per_call  # Default (None) *should* minimise API calls, unsure on this.
    )

    print(rule)

    res_stream = ResultStream(
        rule_payload=rule,
        max_results=max_results,
        max_pages=max_pages,
        **premium_search_args
    )

    # .stream() "seamlessly handles requests and pagination for a given query"
    # It returns a generator
    return res_stream.stream()


def _counts(pt_rule, to_date=None, from_date=None, count_bucket='day'):
    """
    :param pt_rule: PowerTrack rule
    :param to_date: "YYYY-mm-DD HH:MM" format (can exclude time)
    :param from_date: "YYYY-mm-DD HH:MM" format (can exclude time)

    Caveat - premium sandbox environments do NOT have access to the Search API counts endpoint

    :return: list of counts dictionaries for each time period bucket (e.g. for each day), will be a
        max of 30 elements regardless of bucketing strategy. Time period is "YYYYmmDDHHMM" format.

        Example:
            [
                {'count': 366, 'timePeriod': '201801170000'},
                {'count': 44580, 'timePeriod': '201801160000'},
                {'count': 61932, 'timePeriod': '201801150000'},
                ...
            ]
    """
    count_rule = gen_rule_payload(
        pt_rule=pt_rule,
        to_date=to_date,
        from_date=from_date,
        count_bucket=count_bucket,
        results_per_call=RESULTS_PER_CALL
    )

    return collect_results(count_rule, max_results=MAX_RESULTS, result_stream_args=premium_count_args)


def simple_search_to_powertrack(terms, lang=None, limit=POWERTRACK_QUERY_LIMIT):
    pt_query = ' OR '.join(terms)

    if lang:
        pt_query = '({}) lang:{}'.format(pt_query, lang)

    assert len(pt_query) <= limit, 'power track query exceeds max character length of {} with ' \
                                   '{} characters!'.format(POWERTRACK_QUERY_LIMIT, len(pt_query))
    return pt_query


def assert_unique_query_terms(terms):
    lower_term_counts = Counter(map(str.lower, terms))
    duplicates = [k for k, v in lower_term_counts.items() if v > 1]
    assert len(duplicates) == 0, 'found {} duplicated terms: {}'.format(len(duplicates), duplicates)


def deduplicate_query_terms(terms):
    lower_term_counts = Counter(map(str.lower, terms))
    duplicates = [k for k, v in lower_term_counts.items() if v > 1]

    if duplicates:
        print('Removing {} duplicate term(s): {}'.format(len(duplicates), duplicates))

    return sorted(lower_term_counts.keys())


def load_search_terms(file_path):
    terms = []
    with open(file_path, 'r') as fp:
        for line in fp:
            terms.append(line.strip())
    return terms


def split_search_term_file(file_path, limit=1024):
    terms = load_search_terms(file_path)
    terms = deduplicate_query_terms(terms)

    subset_idx = 1
    term_subset = defaultdict(list)

    for term in terms:
        query_len = len(simple_search_to_powertrack(term_subset[subset_idx] + [term], limit=10000000))

        if query_len <= limit:
            term_subset[subset_idx].append(term)
        else:
            subset_idx += 1

    for subset_idx, terms in term_subset.items():
        with open('{}_subset{}'.format(file_path, subset_idx), 'w') as fp:
            for term in terms:
                fp.write('{}\n'.format(term))

    return term_subset


def tweets_to_file(tweets, output_path, log_every=100):
    tweet_count = 0

    with codecs.open(output_path, 'wb', 'utf-8') as fp:
        for t in tweets:
            fp.write('{}\n'.format(json.dumps(t)))

            tweet_count += 1
            if tweet_count % log_every == 0:
                print('Written {} Tweets...'.format(tweet_count))

    print('Written {} Tweets. (Finished)'.format(tweet_count))


def simple_query_search(search_term_path, from_date, to_date, lang=None):
    """
    :param search_term_path: path to file containing line separated search terms, these will be used for
        a simple "OR" query. Each must be a single term, no spaces.
    :param from_date: "YYYY-mm-DD HH:MM" format (can exclude time)
    :param to_date: "YYYY-mm-DD HH:MM" format (can exclude time)

    :return: generator over tweet objects
    """

    # Load search terms and convert to PowerTrack query
    search_terms = load_search_terms(search_term_path)
    search_terms = deduplicate_query_terms(search_terms)
    assert_unique_query_terms(search_terms)
    power_track_query = simple_search_to_powertrack(search_terms, lang=lang)

    print('power_track_query: "{}"'.format(power_track_query))
    print('power_track_query_length: {}'.format(len(power_track_query)))

    return _search(power_track_query, from_date=from_date, to_date=to_date,
                   max_results=MAX_RESULTS, max_pages=MAX_PAGES, results_per_call=RESULTS_PER_CALL)


def simple_query_counts(search_term_path, from_date, to_date, lang=None):
    """
    :param search_term_path: path to file containing line separated search terms, these will be used for
        a simple "OR" query. Each must be a single term, no spaces.
    :param from_date: "YYYY-mm-DD HH:MM" format (can exclude time)
    :param to_date: "YYYY-mm-DD HH:MM" format (can exclude time)

    :return: list of count dictionaries, one for each day that query spans.

        Example:
            [
                {'count': 366, 'timePeriod': '201801170000'},
                {'count': 44580, 'timePeriod': '201801160000'},
                {'count': 61932, 'timePeriod': '201801150000'},
                ...
            ]
     """
    # Load search terms and convert to PowerTrack query
    search_terms = load_search_terms(search_term_path)
    search_terms = deduplicate_query_terms(search_terms)
    assert_unique_query_terms(search_terms)
    power_track_query = simple_search_to_powertrack(search_terms, lang=lang)

    print('power_track_query: "{}"'.format(power_track_query))
    print('power_track_query_length: {}'.format(len(power_track_query)))

    return _counts(power_track_query, from_date=from_date, to_date=to_date, count_bucket='day')


if __name__ == '__main__':

    # Search config
    SEARCH_TERM_FILE = 'terms'
    FROM_DATE = '2018-01-01'
    TO_DATE = '2018-01-01'
    OUTPUT_DIR = 'terms/{}_{}_to_{}/'.format(
        str(SEARCH_TERM_FILE.split('/')[-1]),
        str(FROM_DATE).replace('-', ''),
        str(TO_DATE).replace('-', '')
    )
    LANG = None

    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)

    # Counts
    # counts = simple_query_counts(search_term_path=SEARCH_TERM_FILE, from_date=FROM_DATE, to_date=TO_DATE, lang=LANG)
    # print(counts)
    #
    # with open(OUTPUT_DIR + 'counts.json', 'w') as fp:
    #     json.dump(counts, fp, indent=2)
    #
    # # Data
    # tweet_iter = simple_query_search(search_term_path=SEARCH_TERM_FILE, from_date=FROM_DATE, to_date=TO_DATE, lang=LANG)
    # tweets_to_file(tweets=tweet_iter, output_path=OUTPUT_DIR + 'data.json', log_every=500)
