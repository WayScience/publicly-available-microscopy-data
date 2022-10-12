import pandas as pd
import pathlib, sys
from utils.statistics import collect_databank_stats, collect_study_stats

# Define path to extraction_utils directory
parent_dir = str(pathlib.Path(__file__).parents[1])
sys.path.append(parent_dir)
from metadata_extraction.extraction_utils.io import walk


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
    for metadata_path in metadata_files:
        study_name = ((str(metadata_path).split("/")[-1]).split(".")[0]).split("_")[0]
        _ , attribute_elements = collect_study_stats(metadata_path, individual_study_stats, na_cols=[
            "screen_id",
            "study_name",
            "plate_name",
            "plate_id",
            "sample",
        ], study_name=study_name)

        unique_elements_and_counts[study_name] = attribute_elements

    # Convert unique_elements_and_counts dict to Pandas.DataFrame
    unique_elements_and_counts_df = pd.DataFrame.from_dict({(study, elements_counts): user_dict[study][elements_counts] for study in user_dict.keys() for elements_counts in user_dict[study].keys()}, orient = "index")

    stat_results_df = pd.DataFrame(
        data=individual_study_stats,
        columns=["Study_Name", "Attribute", "S", "H", "NME", "J", "E","GC"],
    )

    # Save elements and counts as parquet file
    elements_and_counts_output_file = pathlib.Path(stats_dir, "unique_elements_and_counts.parquet")
    unique_elements_and_counts_df.to_parquet(elements_and_counts_output_file)

    # Save individual stats as parquet file
    indv_studies_output_file = pathlib.Path(stats_dir, "individual_studies_diversity.parquet")
    stat_results_df.to_parquet(indv_studies_output_file)

    # Collect databank stats
    databank_stats = collect_databank_stats(metadata_directory=studies_metadata_dir, na_cols=[])

    # Save databank stats as parquet file
    databank_output_file = pathlib.Path(stats_dir, f"databank_diversity.parquet")
    databank_stats.to_parquet(databank_output_file)
