import pandas as pd
import pathlib, sys
from utils.statistics import collect_databank_stats, collect_study_stats

# Set path to utils directory
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
    for metadata_path in metadata_files:
        collect_study_stats(metadata_path, individual_study_stats, na_cols=[
            "screen_id",
            "study_name",
            "plate_name",
            "plate_id",
            "sample",
        ])

    stat_results_df = pd.DataFrame(
        data=individual_study_stats,
        columns=["Study_Name", "Attribute", "S", "H", "NME", "J", "E","GC"],
    )

    # Save individual stats as parquet file
    output_file = pathlib.Path(stats_dir, f"individual_studies_diversity.parquet.gzip")
    stat_results_df.to_parquet(output_file, compression="gzip")

    # Collect databank stats
    databank_stats = collect_databank_stats(metadata_directory=studies_metadata_dir, na_cols=[])

    # Save databank stats as parquet file
    output_file = pathlib.Path(stats_dir, f"databank_diversity.parquet.gzip")
    databank_stats.to_parquet(output_file, compression="gzip")
