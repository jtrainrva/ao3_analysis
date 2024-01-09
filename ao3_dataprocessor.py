import AO3
import AO3.works

import time
import pickle
import pickle
import pandas as pd
from itertools import zip_longest
from math import ceil
import glob
import zipfile
import os
import warnings
import json
warnings.filterwarnings("ignore")

script_dir = os.path.dirname(__file__)
tags_dir = os.path.join(script_dir,'tags')
works_dir = os.path.join(script_dir,'works')
relational_dir = os.path.join(script_dir,'relational')
rel_path = "queried_works_dict_list.pkl"
with open(os.path.join(works_dir, rel_path), "rb") as file:
    pickled_data=file.read()
    #pickled_data = blosc.decompress(compressed_pickle)
    dict_list = pickle.loads(pickled_data)
    

#files_to_process = glob.glob('tagCache_compressed_part*.dat')
#for f in files_to_process:
#    with open(f, "rb") as file:
#        
#        compressed_pickle=file.read()
#        pickled_data = blosc.decompress(compressed_pickle)
#        AO3.Tag._cache.update(pickle.loads(pickled_data))

#with open("tagCache_compressed_final.dat", "rb") as file:
#    compressed_pickle=file.read()
#    pickled_data = blosc.decompress(compressed_pickle)
#
#    AO3.Tag._cache = pickle.loads(pickled_data)


rel_path = "tagCache_uncompressed_final.pkl"
with open(os.path.join(tags_dir, rel_path), "rb") as file:
    AO3.Tag._cache = pickle.load(file)
    
# Hard coded fixes
# if a tag was corrected, i've corrected it here
# if it was deleted, it leaves it as an error

#AO3.Tag._cache[r"Arenia Robin Ermal"] = AO3.Tag(r"Arenia 'Robin' Ermal")
#AO3.Tag._cache[r"Male / Female Relationships"] = AO3.Tag(r"Male/Female Relationships")
#AO3.Tag._cache[r"Past Gale (Baldur's Gate)/Mystra (Dungeons & Dragons) - Freeform"] = AO3.Tag(r"Past Gale (Baldur's Gate)/Mystra (Dungeons & Dragons)")
#AO3.Tag._cache[r"NSFWhumptober 2022"] = AO3.Tag(r"NSFWhumptober 2021")
#AO3.Tag._cache[r"The Valkyries (A Court of Thorns and Roses) - Freeform"] = AO3.Tag(r"The Valkyries (A Court of Thorns and Roses)")
#AO3.Tag._cache[r"So Many 90's and early 2000's Pop Culture References"] = AO3.Tag(r"90's and early 2000 Pop Culture References")
#AO3.Tag._cache[r"sylvain wears the im sorry women hat"] = AO3.Tag(r"sylvain wears the i'm sorry women hat")


# For each work, extract metadata and access time

def isCrossover(work):
    all_metatags =  [t.get_metatags()+[t] for t in work.tags_unified if not t.query_error and t.canonical and t.category == 'Fandom']
    if all_metatags:
        intersect = set.intersection(*map(set,all_metatags))
        # Return False if there is a common metatag across Fandom tags
        return not bool(intersect)
    else:
        # No Fandom, return True
        return False

data_dict = {}

normal_fields = (
    "bookmarks", 
    "categories",
    "nchapters",
    "characters",
    "complete",
    "comments",
    "expected_chapters",
    "fandoms",
    "hits",
    "kudos",
    "language",
    "rating",
    "relationships",
    "restricted",
    "status",
    "summary",
    "tags",
    "title",
    "warnings",
    "id",
    "words",
    "collections"
)
datetime_fields = (
    "date_edited",
    "date_published",
    "date_updated",
    "date_queried"
)

for f in normal_fields:
    data_dict[f] = []
    
for f in datetime_fields:
    data_dict[f] = []
    
data_dict['canonical_tags'] = []
data_dict['query_batch'] = []
data_dict['crossover'] = []
    

def safe_attribute(o,attr):
    try:
        return getattr(o,attr)
    except:
        return pd.NA

counter = 0
total_entries = sum(sum(len(i) for i in inner_list) for inner_list in dict_list)


# this queriers AO3 every time an unrecognized tag is found
# it took me about a week to run the first time, but if
# the tag cache is loaded it takes about a minute
works_with_errors = set()
for batch_idx in range(0,len(dict_list)):
    inner_list = dict_list[batch_idx]
    for works_dict in inner_list:
        for work in works_dict.values():
            counter+=1
            print(f"Processing work #{counter} of {total_entries} ({(100*counter)//total_entries}% done) Cache Accessed {AO3.Tag.getCacheAccesses()} times.")
            for att in normal_fields:
                data_dict[att].append(safe_attribute(work,att))
            for att in datetime_fields:
                out = safe_attribute(work,att)
                if pd.isna(out):
                    data_dict[att].append(pd.NA)
                else:
                    data_dict[att].append(pd.Timestamp(out))
            data_dict['canonical_tags'].append(work.search_tags)
            data_dict['query_batch'].append(batch_idx)
            data_dict['crossover'].append(isCrossover(work))
            for t in work.tags_unified:
                if t.loaded and t.query_error:
                    works_with_errors.add(work)
            
          

for work in works_with_errors:
    print(work.id)
    for tag in work.tags_unified:
        if tag.query_error:
            print(f"\t{tag}")

# Note Works 10428609 and 51536557 were deleted

# the search_tags method doesn't load each tag if finds
# to make the tag dataset nicer, i decided to go ahead and load
# the ones it skipped
# shouldnt do anything if cache was loaded
tags =  list(AO3.Tag._cache.values())
for t in tags:
    if not t.loaded:
        print(t)
        t.reload()
            
with AO3.Tag._cache_lock:
    pickled_data = pickle.dumps(AO3.Tag._cache)

rel_path = "tagCache_uncompressed_final.pkl"
with open(os.path.join(tags_dir, rel_path), "wb") as file:
    file.write(pickled_data)

tags_json = json.dumps([t.metadata for t in AO3.Tag._cache.values()])
rel_path = "tag_cache.json"
with open(os.path.join(tags_dir, rel_path), "w") as file:
    file.write(tags_json)
    
zipfile.ZipFile(os.path.join(tags_dir, 'tag_cache_json.zip'), mode='w').write(os.path.join(tags_dir, rel_path),compress_type=zipfile.ZIP_DEFLATED)


works_json = json.dumps([[[w.metadata | {'search_tags' : w.search_tags} for w in d.values()] for d in inner_list] for inner_list in dict_list])

rel_path = "works.json"
with open(os.path.join(works_dir, rel_path), "w") as file:
    file.write(works_json)
    
zipfile.ZipFile(os.path.join(tags_dir, 'works_json.zip'), mode='w').write(os.path.join(works_dir, rel_path),compress_type=zipfile.ZIP_DEFLATED)


tags =  list(AO3.Tag._cache.values())

meta_list = [t.metadata for t in tags if (not t.query_error and t.canonical)]
union_dict = {}
key_list = ['name','category','date_queried']
for k in key_list:
    union_dict[k]=[t[k] for t in meta_list]
    
tags_df = pd.DataFrame(union_dict)
tags_df.set_index('name',inplace=True)
         
rel_path = "tags_df.zip"
temp_path=os.path.join(relational_dir, rel_path)
tags_df.to_csv(temp_path,index=True,compression={'method':'zip','compression' : zipfile.ZIP_DEFLATED})

rel_path = "tags_df.csv"
temp_path=os.path.join(relational_dir, rel_path)
tags_df.to_csv(temp_path,index=True)
   


# populate the dataframe
    
works_df = pd.DataFrame.from_dict(data_dict)
# Rename 'tags'
works_df.rename(columns={'tags':'additional_tags'},inplace=True)

# Missing values for these are just zeros
works_df.bookmarks.replace(pd.NA,0,inplace=True)
works_df.comments.replace(pd.NA,0,inplace=True)
works_df.kudos.replace(pd.NA,0,inplace=True)
works_df.hits.replace(pd.NA,0,inplace=True)

# Hard code this one work that for some reason has not word count
# Word count done by copy and pasting it into MS Word
works_df.words[works_df.id.isin([51439114])]=[14413,14413]
# This one for some reason didnt have a word count, but has words as of 12/7/2023,
# but it shows a date updated of 2023-12-02
# I've updated it to the current total
works_df.words[works_df.id.isin([51528592])]=[563,563]
works_df.words.astype(int,copy=False)

# Last three queries were bugged
# They started a day late and had 2 hour gaps between queries
# This line removes them from the dataset
# May cut later
#works_df = works_df.loc[works_df.query_batch.isin(range(0,20))]


# when a work is first observed, if it's updated again
# BEFORE the 24 hour period runs out weird stuff can happen
# Works can also be deleted or made private after the first observation
 


works_df.n_fandoms = works_df.fandoms.apply(len)
works_df.n_additional_tags = works_df.additional_tags.apply(len)

min_dates = []
for u in range(0,23):
    temp_df = temp_df = works_df.loc[works_df['query_batch']==u]
    min_dates.append(temp_df.date_queried.min())


unique_works = works_df['id'].unique()

multi_obs_works = []
for w in unique_works:
    temp_df = works_df.loc[works_df['id']==w]
    n_unique = temp_df.query_batch.unique().shape[0]
    if n_unique>1 and temp_df.shape[0]>=n_unique*2:
        multi_obs_works.append(w)

nobs = [sum(works_df['id']==u) for u in unique_works]

#temp_df = works_df.loc[works_df['id']==51525724]
#min_time_idx = temp_df['date_queried'].argmin()
#max_time_idx = temp_df['date_queried'].argmax()

temp_df = works_df.loc[works_df['id']==51479029]

# Taking the last period
# This should be an hour between queries, but we'll double check

change_dict = {}
# Intialize
change_dict['id'] = []

change_dict['kudos_delta'] = []
change_dict['hits_delta'] = []
change_dict['comments_delta'] = []
change_dict['bookmarks_delta'] = []
change_dict['words_delta'] = []
change_dict['chapters_delta'] = []

change_dict['modified_before_window'] = []

change_dict['hits'] = []
change_dict['kudos'] = []
change_dict['comments'] = []
change_dict['bookmarks'] = []

change_dict['words'] = []
change_dict['chapters'] = []
change_dict['language'] = []
change_dict['restricted'] = []
change_dict['complete'] = []
change_dict['crossover'] = []

change_dict['date_queried'] = []
change_dict['time_elapsed'] = []

change_dict['canonical_tags'] = []

change_dict['query_batch'] = []
change_dict['id-query'] = []

counter=0

orphaned_queries = []
extra_queries = []

for c_id in unique_works:
    temp_df = works_df.loc[works_df['id']==c_id]
    
    unique_queries = temp_df.query_batch.unique()
    
    for query in unique_queries:
        temp_df2 = temp_df.loc[temp_df['query_batch']==query]

        # Check to see if we have both observations from the query
        if temp_df2.shape[0]==1:
            # Orphaned observation
            # The author either:
            #   - deleted the work
            #   - made the work private
            #   - or updated it before the 24 hour time period ran up
            # Add this observation to the orphans list
            orphaned_queries.append(temp_df2.index)
        elif temp_df2.shape[0]==2:

            change_dict['id'].append(c_id)
            change_dict['query_batch'].append(query)
            
            change_dict['id-query'].append(str(c_id)+"-"+str(query))

            change_dict['modified_before_window'].append(len(temp_df)>2)
            
            starting_work = temp_df2.iloc[0]
            final = temp_df2.iloc[1]

            change_dict['kudos_delta'].append(final.kudos-starting_work.kudos)
            change_dict['hits_delta'].append(final.hits-starting_work.hits)
            change_dict['comments_delta'].append(final.comments-starting_work.comments)
            change_dict['bookmarks_delta'].append(final.bookmarks-starting_work.bookmarks)
            change_dict['words_delta'].append(final.words-starting_work.words)
            change_dict['chapters_delta'].append(final.nchapters-starting_work.nchapters)

            change_dict['time_elapsed'].append(final.date_queried-starting_work.date_queried)


            change_dict['hits'].append(starting_work.hits)
            change_dict['kudos'].append(starting_work.kudos)
            change_dict['comments'].append(starting_work.comments)
            change_dict['bookmarks'].append(starting_work.bookmarks)

            change_dict['words'].append(starting_work.words)
            change_dict['chapters'].append(starting_work.nchapters)
            change_dict['language'].append(starting_work.language)
            change_dict['restricted'].append(starting_work.restricted)
            change_dict['complete'].append(starting_work.complete)
            change_dict['crossover'].append(starting_work.crossover)
            
            change_dict['canonical_tags'].append(starting_work.canonical_tags)

            change_dict['date_queried'].append(starting_work.date_queried)
        else:
            # Observed the same work 3 times in a query
            # This should NEVER happen
            # but if it does, print something and note the work_id
            print(f"Observed work {c_id} in query batch {query} {temp_df2.shape[0]} times.")
            extra_queries.append((c_id,query))


change_df = pd.DataFrame.from_dict(change_dict)
change_df.set_index('id-query',inplace=True)

exploded_tags = change_df[["canonical_tags"]].explode("canonical_tags")
exploded_tags.rename(columns={'canonical_tags':'Tag Name'},inplace=True)

rel_path = "exploded_tags.zip"
temp_path=os.path.join(relational_dir, rel_path)
exploded_tags.to_csv(temp_path,index=True,compression={'method':'zip','compression' : zipfile.ZIP_DEFLATED})

rel_path = "exploded_tags.csv"
temp_path=os.path.join(relational_dir, rel_path)
exploded_tags.to_csv(temp_path,index=True)
   
change_df_out = change_df.drop(columns=['canonical_tags'])

rel_path = "change_df.zip"
temp_path=os.path.join(relational_dir, rel_path)
change_df_out.to_csv(temp_path,index=True,compression={'method':'zip','compression' : zipfile.ZIP_DEFLATED})

rel_path = "change_df.csv"
temp_path=os.path.join(relational_dir, rel_path)
change_df_out.to_csv(temp_path,index=True)
   


change_df.time_elapsed

# Cut off time deltas without sufficient time elapsed
change_df = change_df.loc[change_df.time_elapsed>pd.Timedelta(22,'h')]
change_df2 = change_df.loc[change_df.time_elapsed<pd.Timedelta(24.2,'h')]

change_df.words.apply(pd.isna)


# Messing around
change_df['kudos_ratio'] = change_df.kudos_delta/(change_df.hits_delta+1)

change_df['kudos_ratio2'] = change_df.kudos/(change_df.hits)


change_df_popular = change_df.loc[change_df.hits_delta>=100]
change_df_popular = change_df_popular.loc[change_df_popular.kudos_delta>=1]
change_df_popular = change_df_popular.loc[change_df.words>=60000]
change_df_popular['kudos_ratio'] = change_df_popular.kudos_delta/(change_df_popular.hits_delta+1)


change_df_longfics = change_df.loc[(change_df.words>=40000) &
                                   (change_df.language=='English') &
                                   (change_df.kudos_delta==0) &
                                    (change_df.kudos_ratio2<0.005)]



import numpy as np
import matplotlib.pyplot as plt


box_df = {u:np.log10(change_df.hits_delta.loc[change_df.query_batch==u]+1) for u in range(0,21)}

box_df = {u:np.log10(change_df.hits_delta.loc[change_df.query_batch==u]+1) for u in range(0,24)}


fig, ax = plt.subplots()
plt.title('Log10 Change in Hits by Query')
ax.boxplot(box_df.values())
ax.set_xticklabels(box_df.keys())
plt.savefig('loghits_boxplot.png')