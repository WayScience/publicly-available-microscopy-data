import pandas as pd
import pathlib, sys
from utils.statistics import stats_pipeline

parent_dir = str(pathlib.Path(__file__).parents[1])
sys.path.append(parent_dir)
from metadata_extraction.extraction_utils.io import walk


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


def collect_databank_stats(metadata_directory, na_cols=["pixel_size_x", "pixel_size_y"]):
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

        s, h, nme_result, j, e, gc = stats_pipeline(attribute_elements=attribute_elements)

        # Append stats to attribute_results
        results_list.append([attribute, s, h, nme_result, j, e, gc])

    stat_results_df = pd.DataFrame(
        data=results_list, columns=["Attribute", "S", "H", "NME", "J", "E", "GC"]
    )

    return stat_results_df

# Define study metadata directory
studies_metadata_dir = pathlib.Path("IDR/data/metadata")

# Collect metadata file paths
metadata_files = list(walk(studies_metadata_dir))

# Make directories
stats_dir = pathlib.Path("IDR/data/statistics")
pathlib.Path.mkdir(stats_dir, exist_ok=True)

# Calculate statistics for each image attribute for individual studies
individual_study_stats = list()
for metadata_path in metadata_files:
    collect_study_stats(metadata_path, individual_study_stats)

stat_results_df = pd.DataFrame(
    data=individual_study_stats,
    columns=["Study_Name", "Attribute", "S", "H", "NME", "J", "E","GC"],
)

# Save individual stats as parquet file
output_file = pathlib.Path(stats_dir, f"individual_studies_diversity.parquet.gzip")
stat_results_df.to_parquet(output_file, compression="gzip")

# Collect databank stats
databank_stats = collect_databank_stats(metadata_directory=studies_metadata_dir)

# Save databank stats as parquet file
output_file = pathlib.Path(stats_dir, f"databank_diversity.parquet.gzip")
databank_stats.to_parquet(output_file, compression="gzip")
