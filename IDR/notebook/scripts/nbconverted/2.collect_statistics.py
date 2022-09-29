#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import pathlib
from scripts.stat_utils import collect_study_stats, collect_databank_stats
from scripts.io_utils import walk


# In[2]:


# Define study metadata directory
studies_metadata_dir = pathlib.Path("../data/metadata")

# Collect metadata file paths
metadata_files = list(walk(studies_metadata_dir))

# Calculate statistics for each image attribute within each study
all_results_list = list()
for metadata_path in metadata_files:
    collect_study_stats(metadata_path, all_results_list)

stat_results_df = pd.DataFrame(
    data=all_results_list,
    columns=["Study_Name", "Attribute", "S", "H", "NME", "J", "E", "GC"],
)

# Make directories
stats_dir = pathlib.Path("../data/statistics")
pathlib.Path.mkdir(stats_dir, exist_ok=True)

# Save individual stats as parquet file
output_file = pathlib.Path(stats_dir, f"individual_studies_diversity.parquet.gzip")
stat_results_df.to_parquet(output_file, compression="gzip")


# In[3]:


# Collect databank stats
databank_stats = collect_databank_stats(metadata_dir=studies_metadata_dir)

# Save databank stats as parquet file
output_file = pathlib.Path(stats_dir, f"databank_diversity.parquet.gzip")
databank_stats.to_parquet(output_file, compression="gzip")


# In[4]:


study_stats = pd.read_parquet(f"{stats_dir}/individual_studies_diversity.parquet.gzip")
print(study_stats.head())

databank_stats = pd.read_parquet(f"{stats_dir}/databank_diversity.parquet.gzip")
print(databank_stats.head())
