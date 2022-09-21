import pandas as pd
import pathlib
import multiprocessing
import time
from utils.io import walk
from utils.statistics import stats_pipeline


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
