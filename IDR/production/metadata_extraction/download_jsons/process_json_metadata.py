import pathlib
import json
import time
import sys, os
import pandas as pd
import multiprocessing

# Define path to extraction_utils dir
parent_dir = str(pathlib.Path(__file__).parents[1])
sys.path.append(parent_dir)
from extraction_utils.io import walk
from extraction_utils.clean_channels import clean_channel
from extraction_utils.list_modifications import iterate_through_values


def pull_json_well_metadata(
    well_metadata_file,
    image_attributes,
):
    """Pull metadata from downloaded json metadata files

    Parameters
    ----------
    well_metadata_file: pathlib.Path
        Path to json well metadata file

    image_attributes: list
        Image attribute categories
            - Choose from [
            "Channels",
            "Organism",
            "Cell Line",
            "Oraganism Part",
            "Strain",
            "Gene Identifier",
            ]

    Returns
    -------
    metadata: dict
        Metadata values for attribute in image_attributes per well
    """
    # Get plate and well IDs from file path
    plate_id = str(well_metadata_file).split("/")[-2]
    well_id = str(well_metadata_file).split("/")[-1].removesuffix(".json")

    # Load json file as dictionary
    with well_metadata_file.open() as data_file:
        json_dict = json.load(data_file)

    well_results_dict = dict()
    annotations = json_dict["annotations"]
    annotation_values = iterate_through_values(annotations=annotations)
    # Iterate through image attributes
    for image_attribute in image_attributes:
        # Ensure that user-selected image attributes are in annotation values
        if image_attribute in annotation_values:
            # Clean channels
            if image_attribute == "Channels":
                well_results_dict[image_attribute] = clean_channel(annotation_values[image_attribute])
            else:
                well_results_dict[image_attribute] = annotation_values[image_attribute]
        else:
            well_results_dict[image_attribute] = "Not listed"

    well_results_dict["plate_id"] = plate_id
    well_results_dict["well_id"] = well_id
    
    return well_results_dict


def collect_metadata(screen_id, idr_name, imaging_method, sample):
    """Extract metadata per well from downloaded IDR json annotation files. 
    
    Parameters
    ----------
    screen_id: int
    IDR internal ID for each screen

    idr_name: str
    IDR study name (accession code)

    imaging_method: str
    Imaging method used for the screen (Ex. fluorescence microscopy)

    sample: str
    Denotes cell or tissue.

    Returns
    -------
    Saves extracted metadata as .parquet file
    """
    metadata_dir = pathlib.Path("IDR/data/metadata")
    if metadata_dir.exists() == False:
        pathlib.mkdir(metadata_dir)

    split_idr_name = idr_name.split("/")
    study_name = split_idr_name[0]
    screen_name = split_idr_name[1]

    # Make metadata subdirectories
    study_dir = pathlib.Path(metadata_dir, study_name)
    screen_dir = pathlib.Path(study_dir, screen_name)
    pathlib.Path.mkdir(study_dir, exist_ok=True)
    pathlib.Path.mkdir(screen_dir, exist_ok=True)

    # Create iterable list of json metadata files per screen
    json_metadata_screen_dir = pathlib.Path(f"IDR/data/json_metadata/{screen_id}")
    json_metadata_files = list(walk(json_metadata_screen_dir))

    image_attributes=["Channels",
                "Organism",
                "Cell Line",
                "Oraganism Part",
                "Strain",
                "Gene Identifier",
                "Phenotype Identifier",
                "plate_id",
                "well_id",]

    # Collect data
    screen_results_dict = {image_attribute: list() for image_attribute in image_attributes}
    # Iterate through all well json metadata files
    for well_json_metadata in json_metadata_files:
        well_results_dict = pull_json_well_metadata(well_metadata_file=well_json_metadata, image_attributes=image_attributes)
        # Add metadata values per well to final results dictionary
        for image_attribute in well_results_dict.keys():
            screen_results_dict[image_attribute].append(well_results_dict[image_attribute])

    # Convert results dict to pd.DataFrame
    screen_results_df = pd.DataFrame.from_dict(screen_results_dict)

    # Append external metadata values per screen
    external_metadata = {"screen_id": screen_id, "Imaging Method": imaging_method, "Sample": sample}
    for image_attribute in external_metadata.keys():
        screen_results_df[image_attribute] = pd.Series([external_metadata[image_attribute] for index_ in range(len(screen_results_df.index))])
        
    # Save data per IDR accession name
    output_file = pathlib.Path(
        screen_dir, f"{study_name}_{screen_name}_{screen_id}.parquet"
    )
    screen_results_df.to_parquet(output_file)
    

if __name__ == "__main__":

    # Load screen details
    data_dir = pathlib.Path("IDR/data")
    screen_details_file = pathlib.Path(data_dir, "screen_details.parquet")

    # Load pertinant columns of screen details as pandas df
    screen_details_df = pd.read_parquet(screen_details_file)[["screen_id", "idr_name", "Imaging Method", "Sample Type"]]

    # Get available downloaded json screens
    json_metadata_dir = pathlib.Path("IDR/data/json_metadata")
    available_screens = list()
    for screen_path in json_metadata_dir.iterdir():
        split_path = str(screen_path).split("/")
        available_screens.append(int(split_path[-1]))

    study_metadata = list(screen_details_df.itertuples(index=False, name=None))

    # Remove screens that are not downloaded
    study_metadata = [metadata for metadata in study_metadata if metadata[0] in available_screens]

    # Construct multiprocessing Pool object
    multiprocessing_iterable = list()
    available_cores = len(os.sched_getaffinity(0))
    pool = multiprocessing.Pool(processes=available_cores)

    # Begin metadata collection
    start = time.time()
    print(f"Extracting metadata from {len(study_metadata)} screens.")
    pool.starmap(func=collect_metadata, iterable=study_metadata)

    # Close multiprocess pool
    pool.close()
    pool.join()
    print(f"\nMetadata collected. Running cost is {(time.time()-start)/60:.1f} min.")
