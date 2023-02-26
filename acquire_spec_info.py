import json
import sqlite3
import time
import datetime
import logging
from auth import client_id, client_secret
from blizzardapi import BlizzardApi
import multiprocessing
from multiprocessing import Pool, TimeoutError
from ratelimit import limits, RateLimitException, sleep_and_retry

    
# Rate Limiting
# doesn't seem to be aware of multiprocessing
# actual rate limit is 100 per second, 36000 per hour

# this is thus conservatively set at 5 calls per second

ONE_SECOND = 1
MAX_CALLS_PER_SECOND = 1

api_client = BlizzardApi(client_id, client_secret)

query = 'SELECT entry_id, lower(character_name), lower(server), region FROM ladder WHERE (character_spec IS NULL OR character_class IS NULL)'
    
stmt = 'UPDATE ladder SET character_race = ?, character_class = ?, character_spec = ? WHERE entry_id = ?'

conn = sqlite3.connect('ladder.db')
c = conn.cursor()
    
@sleep_and_retry
@limits(calls=MAX_CALLS_PER_SECOND, period=ONE_SECOND)
def make_call(k):
    print("make_call %s" % str(k))
    # Make the call
    try:
        result = api_client.wow.profile.get_character_profile_summary(k[3], "en-US", k[2], k[1])
    except Exception as e:
        # Handle the exception
        print(f'Error: {e}')
        return None
    else:
        # Return the result
        return result

@sleep_and_retry
@limits(calls=MAX_CALLS_PER_SECOND, period=ONE_SECOND)
def make_call_with_retry(k):
    delay = 8
    max_retries = 5
    p = make_call(k)
    while p is None and max_retries > 0:
        print("trying again...", delay, max_retries)
        time.sleep(delay)
        p = make_call(k)
        delay *= 2
        max_retries -= 1

    if p is None:
        print("skipping")
        return
        
    entry_id = k[0]
    if "race" not in p:
        print("info hide", k)
        c.execute(stmt, ("unknown", "unknown", "unknown", entry_id))
        conn.commit()
        return

    character_race = p["race"]["name"]["en_US"]
    character_class = p["character_class"]["name"]["en_US"]
    character_spec = p["active_spec"]["name"]["en_US"]
    c.execute(stmt, (character_race, character_class, character_spec, entry_id))
    print("done with", k)
    conn.commit()
    

if __name__ == '__main__':
    c.execute(query)
    res = c.fetchall()
    
    n = 0
    list_of_k = []
    for k in res:
        list_of_k += [k]

        n += 1

    print(len(list_of_k))

    print(multiprocessing.cpu_count())

    with Pool(processes=5) as pool:
        pool.map(make_call_with_retry, list_of_k)
    
        
    c.close()
