# twitter-premium

### Requirements:
 - python >= 3
 - searchtweets (`pip install searchtweets`) - https://github.com/twitterdev/search-tweets-python

### Setup:
 1. Copy `.twitter_keys_example.yaml` to `.twitter_keys.yaml` and add dev keys
 2. Change `SANDBOX` flag in `premium_search.py` according to use
 3. Change `CRED_YAML_SEARCH_KEY` and `CRED_YAML_COUNT_KEY` in `premium_search.py` to point to the required YAML keys
 4. Create search term list file (one term per line)
 5. Edit `__main__` according to use, example:
 
```python
# Config
SEARCH_TERM_FILE = './searches/search_terms'
FROM_DATE = '2018-12-12'
TO_DATE = '2018-12-24'
OUTPUT_PATH = './data/test_response_data.json'

# To get counts, do:
print(simple_query_counts(search_term_path=SEARCH_TERM_FILE, from_date=FROM_DATE, to_date=TO_DATE))

# To get and save tweets, do:
tweet_iter = simple_query_search(search_term_path=SEARCH_TERM_FILE, from_date=FROM_DATE, to_date=TO_DATE)
tweets_to_file(tweets=tweet_iter, output_path=OUTPUT_PATH, log_every=100)
```

### Notes:
 - https://developer.twitter.com/en/docs/tweets/search/api-reference/premium-search
 - Data endpoint: `https://api.twitter.com/1.1/tweets/search/30day/dev.json`
 - Counts endpoint: `https://api.twitter.com/1.1/tweets/search/30day/dev/counts.json`
