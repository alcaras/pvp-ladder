import json
import pytz
import datetime
import ckwrap
import pandas as pd
from pandas.api.types import CategoricalDtype
import sqlite3
import numpy as np
import math

tanks =  [u'Vengeance Demon Hunter',
          u'Brewmaster Monk',
          u'Blood Death Knight',
          u'Guardian Druid',
          u'Protection Paladin',
          u'Protection Warrior']

healers = [u'Restoration Druid',
           u'Holy Paladin',
           u'Discipline Priest',
           u'Restoration Shaman',
           u'Mistweaver Monk',
           u'Holy Priest',
           u'Preservation Evoker']

melee = [u'Windwalker Monk',
         u'Arms Warrior',
         u'Retribution Paladin',
         u'Outlaw Rogue',
         u'Havoc Demon Hunter',
         u'Subtlety Rogue',
         u'Enhancement Shaman',
         u'Survival Hunter',
         u'Assassination Rogue',
         u'Frost Death Knight',
         u'Fury Warrior',
         u'Unholy Death Knight',
         u'Feral Druid',]

ranged = [u'Frost Mage',
          u'Balance Druid',
          u'Destruction Warlock',
          u'Beast Mastery Hunter',
          u'Affliction Warlock',
          u'Shadow Priest',
          u'Elemental Shaman',
          u'Arcane Mage',
          u'Demonology Warlock',
          u'Marksmanship Hunter',
          u'Fire Mage',
          u'Devastation Evoker']


def which_cluster(k, centers):
    min_i = -1
    min_v = -1   
    for i, c in enumerate(centers):
        if min_i == -1:
            min_i = i
            min_v = abs(k-c)
        if abs(k-c) < min_v:
            min_i = i
            min_v = abs(k-c)
    return min_i


# how do we normalize all?

def process_data(mode="all", region="all"):
    conn = sqlite3.connect('ladder.db')
    c = conn.cursor()


    query_string = "select * from ladder where character_spec not null and character_spec is not 'unknown' and ladder = '%s'" % mode

    if mode != "all":
        if region != "all":
            query_string = "select * from ladder where character_spec not null and character_spec is not 'unknown' and ladder = '%s' and region = '%s'" % (mode, region)
    else:
        if region != "all":
            query_string = "select * from ladder where character_spec not null and character_spec is not 'unknown' and region = '%s'" % (region)
        else:
            query_string = "select * from ladder where character_spec not null and character_spec is not 'unknown'"
   
    
    df = pd.read_sql_query(query_string, conn)
    conn.close()
    
    n_data = len(df)

    # normalize rating

    max_rating = {}

    if mode == "all":
        for mini_mode in ["2v2", "3v3", "rbg"]:
            max_rating[mini_mode] = df.loc[df['ladder'] == mini_mode, 'rating'].max()
            df.loc[df['ladder'] == mini_mode, 'rating'] = (
                df.loc[df['ladder'] == mini_mode, 'rating'] / max_rating[mini_mode] * 3000
            ) # scale to 3000
    
    stats = df.groupby(['character_class', 'character_spec'])['rating'].agg(['mean', 'count', 'std', 'max'])

    ci95_hi = []
    ci95_lo = []
    roles = []

    for i in stats.index:
        m, c, s, _ = stats.loc[i]
        ci95_hi.append(m + 1.96*s/math.sqrt(c))
        ci95_lo.append(m - 1.96*s/math.sqrt(c))
        full_name = "%s %s" % (i[1], i[0])
        role = "Unknown"
        if full_name in tanks:
            role = "Tank"
        elif full_name in healers:
            role = "Healer"
        elif full_name in ranged:
            role = "Ranged"
        elif full_name in melee:
            role = "Melee"
        roles += [role]
   

    stats['ci95_lo'] = ci95_lo
    stats['role'] = roles

    #stats = stats.sort_values(by='ci95_lo', ascending=False)
    # i like sorting by max and using that for clustering better
    stats = stats.sort_values(by='ci95_lo', ascending=False)

    # replace NaN with 0
    stats = stats.fillna(0)

    #print(stats)


    X = stats[['max', 'ci95_lo']].values
    Y = stats['ci95_lo'].values

    stats['norm_max'] = (stats['max'] - 0) / (stats['max'].max() - 0)

    #print(stats)

    stats['norm_ci95_lo'] = (stats['ci95_lo'] - 0) / (stats['ci95_lo'].max() - 0)

    stats['sum_norm_max_ci95_lo'] = stats['norm_max'] + stats['norm_ci95_lo']
    stats = stats.sort_values(by='sum_norm_max_ci95_lo', ascending=False)

    Y = stats['sum_norm_max_ci95_lo'].values

    result = ckwrap.ckmeans(Y, 6)
    centers = result.centers
    
    clusters = []

    for k in Y:
        clusters += [which_cluster(k, centers)]
    
    letters = ["S", "A", "B", "C", "D", "F"]

    ordered_tier_list = CategoricalDtype(
        letters,
        ordered=True
    )

    mapping = {}
    for c in clusters:
        if c not in mapping:
            mapping[c] = letters.pop(0)

    new_clusters = []
    
    for i, q in enumerate(clusters):
        new_clusters += [mapping[q]]


    stats['cluster'] = new_clusters
    stats['cluster'] = stats['cluster'].astype(ordered_tier_list)


    stats = stats.drop(columns=['norm_max', 'norm_ci95_lo'])

    if region == "all":
        region = "all (us, eu, kr)"
    stats = stats.round(2)

    last_updated = datetime.datetime.now().astimezone(pytz.timezone("America/New_York"))
    last_updated_output = str(last_updated)

    rendered = {}
    rendered["last_updated"] = last_updated_output
    rendered["source_url"] = "https://pvp.subcreation.net/"
    rendered["melee_tier_list"] = {"S": [], "A": [], "B": [], "C": [], "D": [], "F": []}
    rendered["ranged_tier_list"] = {"S": [], "A": [], "B": [], "C": [], "D": [], "F": []}
    rendered["tank_tier_list"] = {"S": [], "A": [], "B": [], "C": [], "D": [], "F": []}
    rendered["healer_tier_list"] = {"S": [], "A": [], "B": [], "C": [], "D": [], "F": []}
    rendered["melee_data"] = []
    rendered["ranged_data"] = []
    rendered["tank_data"] = []
    rendered["healer_data"] = []
    rendered["counts"] = {}
    rendered["counts"]["overall"] = n_data
    rendered["counts"]["tank"] = 0
    rendered["counts"]["healer"] = 0
    rendered["counts"]["ranged"] = 0
    rendered["counts"]["melee"] = 0

    for index, row in stats.iterrows():
        pretty_name = "%s %s" % (index[1], index[0])
        slug_name = pretty_name.lower().replace(" ", "-")
        role = row["role"].lower()
        
        rendered[role + "_tier_list"][row["cluster"]] += [pretty_name]
        rendered["counts"][role] += row["count"]
        
        data_to_add = []
        data_to_add += [row["ci95_lo"], pretty_name, row["mean"], row["count"], slug_name,  row["max"], ""]
        
        rendered[role + "_data"] += [data_to_add]

    return rendered

if __name__ == '__main__':
    modes = ["all", "2v2", "3v3", "rbg"]
    regions = ["all"]

    for mode in modes:
        rendered = process_data(mode)
        # write this to a file
        f = open('%s.json' % mode, 'w')
        f.write(json.dumps(rendered) + "\n")
        f.close()
