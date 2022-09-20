#!/usr/bin/env python
# coding: utf-8

# In[15]:


import pandas as pd
import pathlib
import numpy as np
from numpy import log as ln
import scipy.stats as stats
from scipy import ndimage
import multiprocessing
import time


# In[16]:


def walk(path):
    """Collects paths for files in path parameter

    Parameters
    ----------
    path: str or pathlib.Path() object
            Path to metadata folder containing IDR study directories

    Returns
    -------
    PosixPath object
    """
    for subdir in pathlib.Path(path).iterdir():
        if subdir.is_dir():
            yield from walk(subdir)
            continue
        yield subdir.resolve()


# In[17]:


# Define stats
def category_frequencies(attribute_elements):
    """Calculates absolute and relative frequencies for unique elements of an image attribute

    Parameters
    ----------
    attribute_elements: dict
        Unique elements of an image attribute as keys and instances of each as values

    Returns
    -------
    rel_freq_dict: dict
        Relative frequencies per unique image attribute element
    abs_freq-list: list
        Instance counts of unique image attribute elements
    """
    total_instances = sum(attribute_elements.values())
    rel_freq_dict = dict()
    abs_freq_list = list()
    for image_attribute in attribute_elements.keys():
        abs_freq_list.append(attribute_elements[image_attribute])
        rel_freq_dict[image_attribute] = (
            attribute_elements[image_attribute] / total_instances
        )
    return rel_freq_dict, abs_freq_list


def h_index(p):
    """Calculates the Shannon Index of a set of unique attribute instances.

    Parameters
    ----------
    p: dict
        Dictionary of relative frequencies for each unique element in an attribute column.

    Returns
    -------
    h: float
        Shannon Index value
    results: list
        List of each -p_iln(p_i) value to use for Normalized Median Evenness statistic.
    """
    results = list()
    for entry in p.values():
        results.append(entry * ln(entry))

    results = np.array(results)
    h = -(sum(results))
    return h, results


def pielou(h, s):
    """Calculates the ratio of the observed Shannon Index to the maximum possible Shannon Index value

    Parameters
    ----------
    h: float
        Observed Shannon Index calculated by h-index()
    s: int
        Observed richness within an image attribute (count of unique entries)

    Returns
    -------
    j: float
        Pielou's evenness (H_obs / H-max)
    """
    j = h / ln(s)
    return j


def norm_median_evenness(h_list):
    """Calculates Normalized Median Evenness (NME) of Shannon Index summation elements (-p*ln(p))

    Parameters
    ----------
    h_list: list
        Individual -p*ln(p) values of the Shannon Index summation

    Returns
    -------
    nme: float
        Ratio of median and max -p*ln(p) values
    """
    # Multiply each value in h_list by -1
    h_values = np.array([-1.0 * h_value for h_value in h_list])

    # Calculate NME
    nme = ndimage.median(h_values) / h_values.max()
    return nme


def gini_coef(absolute_frequencies_list):
    """Calculates the Gini coefficient of inequality across unique elements per image attribute

    Parameters
    ----------
    absolute_frequencies_list: list
        Counts of instances of each unique element of an image attribute

    Returns
    -------
    gc: float
        Measure of inequality in range [0, 1] where 0 is perfect equality and 1 is perfect inequality
    """
    absolute_frequencies = np.array(absolute_frequencies_list)

    # Integrate the Lorenze Curve
    total = 0
    for count, abs_freq in enumerate(absolute_frequencies[:-1], 1):
        total += np.sum(np.abs(abs_freq - absolute_frequencies[count:]))

    gc = total / (len(absolute_frequencies) ** 2 * np.mean(absolute_frequencies))
    return gc


def stats_pipeline(attribute_elements):
    """Pipeline to calculate all pertinant diversity statistics

    Parameters
    ----------
    attribtute_elements: dict
        Unique elements of an image attribute as keys and instances of each as values

    Returns
    -------
    s: int
        Richness
    h: float
        Shannon Index
    nme: float
        Normalized Median Evenness
    j: float
        Pielou's Evenness
    gc: float
        Gini coefficient
    """
    # Richness
    s = len(attribute_elements.keys())

    # Shannon Index
    rel_frequencies, abs_frequencies = category_frequencies(
        attribute_elements=attribute_elements
    )
    h, pi_list = h_index(p=rel_frequencies)

    # Calculate Normalized Median Evenness
    nme = norm_median_evenness(pi_list)

    # Calculate Pielou's evenness
    j = pielou(h=h, s=s)

    # Calculate Gini coefficient
    gc = gini_coef(abs_frequencies)

    return s, h, nme, j, gc


# In[18]:


def collect_study_stats(
    metadata_file_path,
    results_list,
    na_cols=[
        "screen_id",
        "study_name",
        "plate_name",
        "plate_id",
        "sample",
        "pixel_size_x",
        "pixel_size_y",
    ],
):
    """Collecting statistics within a single file

    Parameters
    ----------
    metadata_file_path: PosixPath object
        Path to single study .parquet metadata file
    results_list: list
        Outside instantiated list to append study statistics
    na_cols: list
        Image attributes excluded from statistical calculations

    Returns
    -------
    results_list: list
        The input results list appended with statistics from the metadata_file_path study
    """
    # Read parquet into pandas df
    metadata_df = pd.read_parquet(metadata_file_path)

    # Extract metadata from file name and dataframe
    metadata_pq = metadata_file_path.name
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

        s, h, nme, j, gc = stats_pipeline(attribute_elements=attribute_elements)

        # Append stats to attribute_results
        results_list.append([study_name, attribute, s, h, nme, j, gc])

    return results_list


# In[19]:


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
    columns=["Study_Name", "Attribute", "S", "H", "NME", "J", "GC"],
)

# Make directories
stats_dir = pathlib.Path("../data/statistics")
pathlib.Path.mkdir(stats_dir, exist_ok=True)

# Save individual stats as parquet file
output_file = pathlib.Path(stats_dir, f"individual_studies_diversity.parquet.gzip")
stat_results_df.to_parquet(output_file, compression="gzip")


# In[20]:


def collect_databank_stats(metadata_dir, na_cols=["pixel_size_x", "pixel_size_y"]):
    """Statistics pipeline for computation accross a databank

    Parameters
    ----------
    metadata_dir: PosixPath object
        Path to the metadata directory containing subdirectories for studies and screens
    na_cols: list
        Image attributes excluded from statistical calculations

    Returns
    -------
    stat_results_df: pandas dataframe
        Contains all statistics for each image attribute not in na_cols calculated across all studies
    """
    metadata_directory = pathlib.Path("../data/metadata")

    # Open and concatinate study metadata dataframes from .parquet files
    databank_metadata = pd.concat(
        [
            pd.read_parquet(study_metadata_file)
            for study_metadata_file in walk(metadata_directory)
        ]
    )

    # Get image_attribute names
    attribute_names = databank_metadata.columns.to_list()

    # Remove irrelevant attributes
    for attribute in na_cols:
        attribute_names.remove(attribute)

    results_list = list()
    # Collect statistics for each attribute
    for attribute in attribute_names:
        unique_entries = databank_metadata[attribute].unique()
        attribute_elements = dict()
        for element in unique_entries:
            attribute_elements[element] = len(
                databank_metadata[databank_metadata[attribute] == element]
            )

        s, h, nme_result, j, gc = stats_pipeline(attribute_elements=attribute_elements)

        # Append stats to attribute_results
        results_list.append([attribute, s, h, nme_result, j, gc])

    stat_results_df = pd.DataFrame(
        data=results_list, columns=["Attribute", "S", "H", "NME", "J", "GC"]
    )

    return stat_results_df


# In[21]:


# Collect databank stats
databank_stats = collect_databank_stats(metadata_dir=studies_metadata_dir)

# Save databank stats as parquet file
output_file = pathlib.Path(stats_dir, f"databank_diversity.parquet.gzip")
databank_stats.to_parquet(output_file, compression="gzip")

