import pathlib
import sys

import pandas as pd
from tqdm import tqdm
from utils.statistics import collect_databank_stats, collect_study_stats

# Define path to extraction_utils directory
parent_dir = str(pathlib.Path(__file__).parents[1])
sys.path.append(parent_dir)
from metadata_extraction.extraction_utils.io import walk


def nested_dict_pairs_iterator(elements_and_counts_dict):
    """Collects all combinations of tuples for a nested dictionary

    Parameters
    ----------
    elements_and_counts_dict: dict
        Nested dictionary of all combinations of all unique entries of a metadata file
        E.g. {study_name: {screen_id: {image_attribute: {unique_element: count}}}}

    Returns
    -------
    All possible combinations of tuples for a nested dictionary
        E.g. (study_name1, screen_id1, image_attribute1, unique_element1, count1)
    """
    # Iterate over all key-value pairs of dict argument
    for key, value in elements_and_counts_dict.items():
        # Check if value is of dict type
        if isinstance(value, dict):
            # If value is dict then iterate over all its values
            for pair in nested_dict_pairs_iterator(value):
                yield (key, *pair)
        else:
            # If value is not dict type then yield the value
            yield (key, value)


if __name__ == "__main__":
    # Define study metadata directory
    studies_metadata_dir = pathlib.Path("IDR/data/metadata")

    # Collect metadata file paths
    metadata_files = list(walk(studies_metadata_dir))

    # Make directories
    stats_dir = pathlib.Path("IDR/data/statistics")
    pathlib.Path.mkdir(stats_dir, exist_ok=True)

    # Calculate statistics for each image attribute for individual studies
    individual_study_stats = list()
    unique_elements_and_counts = dict()

    print(f"\nComputing statistics for {len(metadata_files)} screens.\n")
    
    # Iterate through each study/screen/well.json metadata file
    for metadata_path in tqdm(metadata_files):
        parsed_data_path = ((str(metadata_path).split("/")[-1]).split(".")[0]).split("_")
        study_name = parsed_data_path[0]
        screen_id = parsed_data_path[-1]
        _, attribute_elements = collect_study_stats(
            metadata_path,
            individual_study_stats,
            na_cols=["screen_id", "study_name", "plate_name", "plate_id", "well_id", "Organism Part"],
            study_name=study_name,
            screen_id=screen_id,
        )

        unique_elements_and_counts[study_name] = attribute_elements

    # Convert unique_elements_and_counts dict to Pandas.DataFrame
    unique_elements_and_counts_list = list(
        nested_dict_pairs_iterator(unique_elements_and_counts)
    )

    # Generate output dataframes
    unique_elements_and_counts_df = pd.DataFrame(
        data=unique_elements_and_counts_list,
        columns=["Study", "Screen", "Attribute", "Element", "Count"],
    )
    
    unique_elements_and_counts_df["Element"] = unique_elements_and_counts_df["Element"].apply(
            lambda x: str(x)
        )

    stat_results_df = pd.DataFrame(
        data=individual_study_stats,
        columns=["Study_Name", "Attribute", "S", "H", "NME", "J", "E", "GC"],
    )

    # Save elements and counts as parquet file
    elements_and_counts_output_file = pathlib.Path(
        stats_dir, "unique_elements_and_counts.parquet"
    )
    unique_elements_and_counts_df.to_parquet(elements_and_counts_output_file)

    # Save individual stats as parquet file
    indv_studies_output_file = pathlib.Path(
        stats_dir, "individual_studies_diversity.parquet"
    )
    stat_results_df.to_parquet(indv_studies_output_file)

    # Collect databank stats
    databank_stats = collect_databank_stats(
        metadata_directory=studies_metadata_dir, na_cols=["screen_id", "study_name", "plate_name", "plate_id", "well_id", "Organism Part"]
    )

    # Save databank stats as parquet file
    databank_output_file = pathlib.Path(stats_dir, f"databank_diversity.parquet")
    databank_stats.to_parquet(databank_output_file)
