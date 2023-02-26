from auth import client_id, client_secret
from blizzardapi import BlizzardApi
import pprint
import json
import sqlite3
import logging

# 
PVP_SEASON = 34

def get_pvp_leaderboard(mode, region, retries=0):
    if retries > 5:
        print("failed, giving up on %s" % (mode, region))
        return None

    print("getting pvp leaderboard for %s %s" % (mode, region))
    
    api_client = BlizzardApi(client_id, client_secret)

    ladder = "%s" % (mode)
    print(ladder)
    if retries > 0:
        print("retry #%d" % retries)

    try:
        leaderboard = api_client.wow.game_data.get_pvp_leaderboard(region, "en-US", PVP_SEASON, ladder)
    except json.decoder.JSONDecodeError:
        # try again
        return get_shuffle_leaderboard(spec, c_class, region, retries+1)

    conn = sqlite3.connect('ladder.db')
    c = conn.cursor()
    
    # Access the data
    values = []

    for k in leaderboard["entries"]:
        values.append((mode, k["rating"], k["character"]["id"], k["character"]["realm"]["slug"], k["character"]["name"], None, None, k["faction"]["type"], region, 0))

    c.executemany('INSERT INTO ladder (ladder, rating, character_id, server, character_name, character_class, character_spec, faction, region, fetch_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)', values)

    conn.commit()
    conn.close()
    print("done!")
    return

conn = sqlite3.connect('ladder.db')
c = conn.cursor()

conn.execute("drop table if exists ladder")
conn.execute("CREATE TABLE ladder (entry_id integer primary key autoincrement, ladder TEXT, rating INTEGER, character_id INTEGER, server TEXT, character_name TEXT, character_spec TEXT,character_class TEXT, faction TEXT, region TEXT, fetch_id INTEGER, character_race TEXT)")

conn.commit()
conn.close()

modes = ["2v2", "3v3", "rbg"]

for region in ["us", "eu", "kr"]:
    for mode in modes:
        get_pvp_leaderboard(mode, region)


