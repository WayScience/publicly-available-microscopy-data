import pathlib
import json
import pandas as pd


def pull_metadata_from_json(
    json_file_paths,
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
        List of metadata values for attribute in image_attributes
    """
    json_keys = {
        "Channels": "omero/bulk_annotations",
        "Organism": "mapr/organism",
        "Cell Line": "mapr/cell_line",
        "Oraganism Part": "omero/bulk_annotations",
        "Strain": "omero/bulk_annotations",
        "Gene Identifier": "mapr/gene",
        "Phenotype Identifier": "mapr/phenotype",
    }

    for plate in study_plates:
        imageIDs = list()
        plate_name = study_plates[plate]

        # Access .json file for the plate ID. Contains image ID (id) numbers
        # for replicate images per plate.
        WELLS_IMAGES_URL = f"https://idr.openmicroscopy.org/webgateway/plate/{plate}/"
        grid = session.get(WELLS_IMAGES_URL).json()

        try:
            pixel_size_x = grid["image_sizes"][0]["x"]
            pixel_size_y = grid["image_sizes"][0]["y"]

            # Get well ids
            excluded_keys = ["collabels", "rowlabels", "image_sizes"]
            for key in excluded_keys:
                well_JSON.pop(key, None)
            for plate_row in range(len(well_JSON["grid"])):
                for well in well_JSON["grid"][row]:
                    # Append IDs to iterable lists
                    wellIDs.append(well["wellId"])

        except (ValueError, KeyError):
            plate_results.append(
                [
                    screen_id,
                    plate,
                    plate_name,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                ]
            )
            continue

        ANNOTATIONS_URL = "openmicroscopy.org/"

        # Get image details from each image id for each plate
        for wellID in wellIDs:
            MAP_URL = f"https://idr.openmicroscopy.org/webclient/api/annotations/?type=map&image={id}"
            annotations = session.get(MAP_URL).json()["annotations"]

            for image_attribute in image_attributes:
                ATTRIBUTE_URL = ANNOTATIONS_URL + json_keys[image_attribute]

                try:
                    attribute_index = [x["ns"] for x in annotations].index(
                        ATTRIBUTE_URL
                    )
                    image_attribute_value = {
                        x[0]: x[1] for x in annotations[attribute_index]["values"]
                    }[image_attribute]

                except (ValueError, KeyError):
                    image_attribute_value = "Not Listed"

            # Build results
            plate_results.append(
                [
                    screen_id,
                    study_name,
                    plate,
                    plate_name,
                    wellID,
                    imaging_method,
                    sample,
                    organism,
                    organism_part,
                    cell_line,
                    strain,
                    gene_identifier,
                    phenotype_identifier,
                    stains_targets,
                    pixel_size_x,
                    pixel_size_y,
                ]
            )



pull_metadata_from_json(image_attributes=["Channels",
        "Organism",
        "Cell Line",
        "Oraganism Part",
        "Strain",
        "Gene Identifier"])


def collect_metadata(idr_name, values_list):
    """ """
    data_dir = pathlib.Path("IDR/data/metadata")
    if data_dir.exists() == False:
        os.mkdir(data_dir)

    screen_id = values_list[0]
    imaging_method = values_list[1]
    sample = values_list[2]
    split_name = idr_name.split("/")
    study_name = split_name[0]
    screen_name = split_name[1]

    # Make directories
    study_dir = pathlib.Path(data_dir, study_name)
    screen_dir = pathlib.Path(study_dir, screen_name)
    if study_dir.exists() == False:
        os.mkdir(study_dir)
    elif screen_dir.exists() == False:
        os.mkdir(screen_dir)
    else:
        pass

    # Collect data
    plate_results_df = describe_screen(
        screen_id=screen_id,
        sample=sample,
        imaging_method=imaging_method,
        study_name=study_name,
    )

    # Save data per IDR accession name
    output_file = pathlib.Path(
        screen_dir, f"{study_name}_{screen_name}_{screen_id}.parquet.gzip"
    )
    plate_results_df.to_parquet(output_file, compression="gzip")
