from email.mime import image
import pathlib
import json
import time
import os
import pandas as pd
from utils.io import walk
from utils.clean_channels import clean_channel
import multiprocessing


def flatten_list(list_):
    return [item for sublist in list_ for item in sublist]

def nested_list_to_dict(nested_list):
    return {k[0]: k[1:][0] for k in nested_list}

def iterate_through_values(annotations):
    values_list = [sub_dict['values'] for sub_dict in annotations]
    flattened_list = flatten_list(values_list)
    values_dict = nested_list_to_dict(flattened_list)
    return values_dict


def pull_json_well_metadata(
    well_metadata_file,
    image_attributes,
):
    """Pull metadata from downloaded json metadata files

    Parameters
    ----------
    image_attributes: list
        Image attribute categories
            - Choose from [
            "Channels",
            "Organism",
            "Cell Line",
            "Oraganism Part",
            "Strain",
            "Gene Identifier",
            "pixel_x",
            "pixel_y"]

    Returns
    -------
    metadata: list
        List of metadata values for attribute in image_attributes per well
    """
    plate_id = str(well_metadata_file).split("/")[-2]
    well_id = str(well_metadata_file).split("/")[-1].removesuffix(".json")

    with well_metadata_file.open() as data_file:
        json_dict = json.load(data_file)

    well_results_dict = dict()
    annotations = json_dict["annotations"]
    annotation_values = iterate_through_values(annotations=annotations)
    for image_attribute in image_attributes:
        if image_attribute in annotation_values:
            if image_attribute == "Channels":
                well_results_dict[image_attribute] = clean_channel(annotation_values[image_attribute])
            else:
                well_results_dict[image_attribute] = annotation_values[image_attribute]
        else:
            well_results_dict[image_attribute] = "Not listed"

    well_results_dict["plate_id"] = plate_id
    well_results_dict["well_id"] = well_id
    
    return well_results_dict



def collect_metadata(metadata_tuple):
    """ """
    data_dir = pathlib.Path("IDR/data/metadata")
    if data_dir.exists() == False:
        pathlib.mkdir(data_dir)

    screen_id = metadata_tuple[0]
    idr_name = metadata_tuple[1]
    imaging_method = metadata_tuple[2]
    sample = metadata_tuple[3]
    split_idr_name = idr_name.split("/")
    study_name = split_idr_name[0]
    screen_name = split_idr_name[1]

    # Make metadata subdirectories
    study_dir = pathlib.Path(data_dir, study_name)
    screen_dir = pathlib.Path(study_dir, screen_name)
    pathlib.Path.mkdir(study_dir, exist_ok=True)
    pathlib.Path.mkdir(screen_dir, exist_ok=True)

    # Create iterable list of json metadata files
    json_metadata_screen_dir = pathlib.Path("IDR/data/json_metadata")
    json_metadata_files = list(walk(json_metadata_screen_dir))

    image_attributes=["Channels",
                "Organism",
                "Cell Line",
                "Oraganism Part",
                "Strain",
                "Gene Identifier"]

    # Collect data
    screen_results_dict = {image_attribute: list() for image_attribute in image_attributes}
    for well_json_metadata in json_metadata_files:
        well_results_list = pull_json_well_metadata(well_metadata_file=well_json_metadata, image_attributes=image_attributes)
        for image_attribute in well_results_list.keys():
            screen_results_dict[image_attribute].append(well_results_list[image_attribute])

    screen_results_df = pd.DataFrame.from_dict(screen_results_dict)
    screen_results_df['Imaging Method'] = pd.Series([imaging_method for x in range(len(screen_results_df.index))])
    screen_results_df['Sample'] = pd.Series([sample for x in range(len(screen_results_df.index))])
        
    # Save data per IDR accession name
    output_file = pathlib.Path(
        screen_dir, f"{study_name}_{screen_name}_{screen_id}.parquet.gzip"
    )
    screen_results_df.to_parquet(output_file, compression="gzip")
    

if __name__ == "__main__":

    # Load screen details
    data_dir = pathlib.Path("IDR/data")
    screen_details_file = pathlib.Path(data_dir, "screen_details.parquet")

    # Load pertinant columns of screen details as pandas df
    screen_details_df = pd.read_parquet(screen_details_file)[["id", "idr_name", "Imaging Method", "Sample"]]

    # Create iterable list for multiprocessing
    study_metadata = list(screen_details_df.itertuples(index=False, name=None))

    # Set up Pool object
    multiprocessing_iterable = list()
    available_cores = len(os.sched_getaffinity(0))
    pool = multiprocessing.Pool(processes=available_cores)

    # Begin metadata collection
    start = time.time()
    pool.starmap(func=collect_metadata, iterable=study_metadata)

    # Close multiprocess pool
    pool.close()
    pool.join()
    print(f"\\nMetadata collected. Running cost is {(time.time()-start)/60:.1f} min.")

    print("Extraction method under development")
