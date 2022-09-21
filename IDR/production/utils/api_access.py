def pull_metadata(
    session,
    study_plates,
    screen_id,
    plate_results,
    image_attributes=[
        "Channels",
        "Organism",
        "Cell Line",
        "Oraganism Part",
        "Strain",
        "Gene Identifier",
    ],
):
    """Pull metadata from API

    Parameters
    ----------
    screen_id:

    plate_results: list


    image_attributes: list
        Image attribute categories
            - Choose from ["Channels", "Organism", "Cell Line", "Oraganism Part", "Strain", "Gene Identifier"]

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

            # Get image ids
            for image in grid["grid"][0]:
                # Append IDs to iterable lists
                thumb_url = image["thumb_url"].rstrip(image["thumb_url"][-1])
                image_id = thumb_url.split("/")[-1]
                imageIDs.append(image_id)

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
                    id,
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
