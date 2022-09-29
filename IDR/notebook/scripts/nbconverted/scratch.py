#!/usr/bin/env python
# coding: utf-8

# In[1]:


from scripts.stat_utils import (
    h_index,
    pielou,
    norm_median_evenness,
    simpsons_e,
    gini_coef,
    stats_pipeline,
    category_frequencies,
)
import random
import pandas as pd
import pathlib
import multiprocessing
import time
import importlib
from scripts.stat_utils import collect_study_stats, collect_databank_stats
from scripts.io_utils import walk


# In[34]:


def Rand(start, end, num):
    res = []

    for j in range(num):
        res.append(random.randint(start, end))

    abs_freq = res
    rel_freq = [(p / sum(res)) for p in res]

    return rel_freq, abs_freq


# Driver Code
num = 10000
start = 1
end = 100
rel_freqs, abs_freq = Rand(start, end, num)

print("Check that add to 1: ", sum(rel_freqs))

s = len(rel_freqs)
print("S: ", s)

h = h_index(rel_freqs)

j = pielou(h=h, s=len(rel_freqs))
print("J': ", j)

nme = norm_median_evenness(rel_freq_list=rel_freqs)
print("NME: ", nme)

e = simpsons_e(rel_freq_list=rel_freqs, s=s)
print("E:", e)

gc = gini_coef(absolute_frequencies_list=abs_freq)
print("GC", gc)


# In[5]:


na_cols = [
    "screen_id",
    "study_name",
    "plate_name",
    "plate_id",
    "sample",
    "pixel_size_x",
    "pixel_size_y",
]

# Define study metadata directory
studies_metadata_dir = pathlib.Path("../data/metadata")

# Collect metadata file paths
metadata_files = list(walk(studies_metadata_dir))

# Calculate statistics for each image attribute within each study
all_results_list = list()
for metadata_path in metadata_files:
    # Read parquet into pandas df
    metadata_df = pd.read_parquet(metadata_path)

    # Extract metadata from file name and dataframe
    metadata_pq = metadata_path.name
    study_name = metadata_pq.split(".")[0]
    attribute_names = metadata_df.columns.to_list()

    # Remove irrelevant attributes
    for attribute in na_cols:
        attribute_names.remove(attribute)

    # Collect statistics for each attribute
    for attribute in attribute_names:
        unique_entries = metadata_df[attribute].unique()
        attribute_elements = dict()
        for element in unique_entries:
            attribute_elements[element] = len(
                metadata_df[metadata_df[attribute] == element]
            )

        rel_frequencies, abs_frequencies = category_frequencies(
            attribute_elements=attribute_elements
        )
        # s, h, nme, j, e, gc = stats_pipeline(attribute_elements=attribute_elements)


# stat_results_df = pd.DataFrame(
#     data=all_results_list,
#     columns=["Study_Name", "Attribute", "S", "H", "NME", "J", "E", "GC"],
# )

# print(stat_results_df)
