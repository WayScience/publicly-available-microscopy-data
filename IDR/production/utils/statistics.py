import numpy as np
import pandas as pd
import pathlib, sys

parent_dir = str(pathlib.Path(__file__).parents[1])
sys.path.append(parent_dir)
from metadata_extraction.extraction_utils.io import walk
from scipy import ndimage
from numpy import log as ln


def category_frequencies(attribute_elements):
    """Calculates absolute and relative frequencies for unique elements of an image attribute
    Parameters
    ----------
    attribute_elements: dict
        Unique elements of an image attribute as keys and instances of each as values
    Returns
    -------
    rel_freq_list: list
        Relative frequencies per unique image attribute element
    abs_freq-list: list
        Instance counts of unique image attribute elements
    """
    total_instances = sum(attribute_elements.values())
    rel_freq_list = list()
    abs_freq_list = list()
    for image_attribute_instances in attribute_elements.values():
        abs_freq_list.append(image_attribute_instances)
        rel_freq_list.append(image_attribute_instances / total_instances)

    return rel_freq_list, abs_freq_list


def h_index(rel_freq_list):
    """Calculates the Shannon Index of a set of unique attribute instances.
    Parameters
    ----------
    p: list
        Relative frequencies for each unique element in an attribute column.
    Returns
    -------
    h: float
        Shannon Index value
    evenness_values: list
        List of each -p_iln(p_i) value to use for Normalized Median Evenness statistic.
    """
    evenness_values = np.array([rel_freq * ln(rel_freq) for rel_freq in rel_freq_list])
    h = -(sum(evenness_values))

    return h


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
    # Prevent division by 0 if richness (S) == 1

    j = h / ln(s)

    return j


def norm_median_evenness(rel_freq_list):
    """Calculates Normalized Median Evenness (NME) of Shannon Index summation elements (-p*ln(p))
    Parameters
    ----------
    rel_freq_list: list
        Relative frequencies of counts for each unique element in an image attribute
    Returns
    -------
    nme: float
        Ratio of median and max -p*ln(p) values
    """
    h_values = np.array(
        [-1.0 * freq * ln(freq) for freq in rel_freq_list if 1 not in rel_freq_list]
    )

    # Calculate NME
    nme = ndimage.median(h_values) / h_values.max()

    return nme


def simpsons_e(rel_freq_list, s):
    """Calculates Simpson's evenness for each unique element per image attribute
    Parameters
    ----------
    rel_freq_list: list
        Relative frequencies of counts for each unique element in an image attribute
    s : int
        Richness --> number of unique elements in an image attribute
    Returns
    -------
    e: float
        Ratio of inverse of Simpson's dominance of a unique element to image attribute richness
    """
    dominance = sum([p**2 for p in rel_freq_list])
    e = (1 / dominance) / s

    return e


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

    # # Shannon Index
    rel_frequencies, abs_frequencies = category_frequencies(
        attribute_elements=attribute_elements
    )

    # If 1 unique element in an image attribute --> rel_frequencies = [1.0]
    # Causes h = -0 and division by 0 in norm_median_evenness and pielou
    if s == 1:
        h = 0
        nme = None
        j = None
    else:
        h = h_index(rel_freq_list=rel_frequencies)
        nme = norm_median_evenness(rel_freq_list=rel_frequencies)
        j = pielou(h=h, s=s)

    e = simpsons_e(rel_freq_list=rel_frequencies, s=s)
    gc = gini_coef(absolute_frequencies_list=abs_frequencies)

    return s, h, nme, j, e, gc


def collect_study_stats(
    metadata_file_path,
    results_list,
    na_cols,
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
        if attribute in attribute_names:
            attribute_names.remove(attribute)
        else:
            pass

    # Collect statistics for each attribute
    for attribute in attribute_names:
        unique_entries = metadata_df[attribute].unique()
        attribute_elements = dict()
        for element in unique_entries:
            attribute_elements[element] = len(
                metadata_df[metadata_df[attribute] == element]
            )

        s, h, nme, j, e, gc = stats_pipeline(attribute_elements=attribute_elements)

        # Append stats to attribute_results
        results_list.append([study_name, attribute, s, h, nme, j, e, gc])

    return results_list


def collect_databank_stats(metadata_directory, na_cols):
    """Statistics pipeline for computation across a databank

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
    # Open and concatenate study metadata dataframes from .parquet files
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
        if attribute in attribute_names:
            attribute_names.remove(attribute)
        else:
            pass

    results_list = list()
    # Collect statistics for each attribute
    for attribute in attribute_names:
        unique_entries = databank_metadata[attribute].unique()
        attribute_elements = dict()
        for element in unique_entries:
            attribute_elements[element] = len(
                databank_metadata[databank_metadata[attribute] == element]
            )

        s, h, nme_result, j, e, gc = stats_pipeline(attribute_elements=attribute_elements)

        # Append stats to attribute_results
        results_list.append([attribute, s, h, nme_result, j, e, gc])

    stat_results_df = pd.DataFrame(
        data=results_list, columns=["Attribute", "S", "H", "NME", "J", "E", "GC"]
    )

    return stat_results_df
