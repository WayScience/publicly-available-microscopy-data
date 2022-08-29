#!/usr/bin/env python
# coding: utf-8

# # Describe study metadata
#
# Each screen contains an experiment with different parameters and conditions.
#
# Extract this information based on ID and save details.

import pathlib
import requests
import pandas as pd


def extract_study_info(session, screen_id):
    """Pull metadata info per screen, given screen id

    Parameters
    ----------
    session: Requests.session()
        Requests session providing access to IDR API
    screen_id: str
        Internal indicator of the specific microscopy data set

    Returns
    -------
    pandas.DataFrame() of metadata per screen id
    """
    base_url = "https://idr.openmicroscopy.org/webclient/api/annotations/"
    screen_url = f"?type=map&screen={screen_id}"

    url = f"{base_url}{screen_url}"
    response = session.get(url).json()

    annotations = response["annotations"]
    study_index = [x["ns"] for x in annotations].index(
        'idr.openmicroscopy.org/study/info')

    id_ = annotations[study_index]["id"]
    date_ = annotations[study_index]["date"]
    name_ = annotations[study_index]["link"]["parent"]["name"]

    details_ = (
        pd.DataFrame({x[0]: x[1]
                      for x in annotations[study_index]["values"]}, index=[0])
        .assign(
            internal_id=id_,
            upload_date=date_,
            idr_name=name_,
            screen_id=screen_id
        )
    )

    return details_


def describe_screen(session, screen_id):
    """Pull additional metadata info per plate, given screen id

    Parameters
    ----------
    session: Requests.session()
        Requests session providing access to IDR API
    screen_id: str
        Internal indicator of the specific microscopy data set

    Returns
    -------
    pandas.DataFrame() of the following metadate values:

        screen_id: IDR ID for the screen
        plate_id: IDR ID for each plate
        plate_name: Names given to each plate
        image_id: IDR ID for each image
        cell_line: Cell line used in the screen experiment
        strain: Strain of the cell line (if specified)
        gene_identifier: Accession code for the gene being perturbed in a well
        phenotype_identifier: Accession code for the phenotype perturbed in a well
        stain: Set of the stains used in the screen
        stain_target: The target protein or media of the stain
        pixel_size_x: Width of the image
        pixel_size_y: Height of the image
    """
    PLATES_URL = f"https://idr.openmicroscopy.org/webclient/api/plates/?id={screen_id}"
    all_plates = session.get(PLATES_URL).json()["plates"]
    study_plates = {x["id"]: x["name"] for x in all_plates}
    print("Number of plates found: ", len(study_plates))

    plate_results = []
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

            # Get image and well ids
            for image in grid['grid'][0]:
                # Append IDs to iterable lists
                thumb_url = image['thumb_url'].rstrip(image['thumb_url'][-1])
                image_id = thumb_url.split('/')[-1]
                imageIDs.append(image_id)

        except (ValueError, KeyError):
            plate_results.append(
                [screen_id, plate, plate_name, None, None, None, None, None])
            continue

        # TODO: Rewrite to optimize and make consice
        # Get image details from each image id for each plate
        for id in imageIDs:
            MAP_URL = f"https://idr.openmicroscopy.org/webclient/api/annotations/?type=map&image={id}"
            annotations = session.get(MAP_URL).json()["annotations"]

            # Get stain and stain target
            try:
                bulk_index = [x["ns"] for x in annotations].index(
                    'openmicroscopy.org/omero/bulk_annotations')
                channels = {x[0]: x[1]
                            for x in annotations[bulk_index]
                            ["values"]}["Channels"]

                # Clean channels value and separate into stain and stain target
                stain = set()
                stain_target = set()
                for entry in channels.split(";"):
                    temp_list = entry.split(":")
                    stain.add(temp_list[0])
                    stain_target.add(temp_list[1])

            except (ValueError, KeyError):
                stain = "Not listed"
                stain_target = "Not listed"

            # Get cell line
            try:
                cell_line_index = [x["ns"] for x in annotations].index(
                    'openmicroscopy.org/mapr/cell_line')
                cell_line = {x[0]: x[1] for x in
                             annotations[cell_line_index]["values"]}["Cell Line"]
            except (ValueError, KeyError):
                cell_line = "Not listed"

            # Get strain of cell line
            try:
                strain = {x[0]: x[1] for x in
                          annotations[bulk_index]["values"]}["Strain"]
            except (ValueError, KeyError):
                strain = "Not listed"

            # Get gene identification accession code
            try:
                gene_identifier_index = int(
                    [x["ns"] for x in annotations].index(
                        'openmicroscopy.org/mapr/gene'))
                gene_identifier = {x[0]: x[1] for x in annotations[gene_identifier_index]["values"]}[
                    "Gene Identifier"]
            except (ValueError, KeyError):
                gene_identifier = "Not Listed"

            # Get phenotype identification accession code
            try:
                phenotype_identifier_index = int(
                    [x["ns"] for x in annotations].index('openmicroscopy.org/mapr/phenotype'))
                phenotype_identifier = {x[0]: x[1] for x in annotations[phenotype_identifier_index]["values"]}[
                    "Phenotype Term Accession"]
            except (ValueError, KeyError):
                phenotype_identifier = "Not Listed"

            # Build results
            plate_results.append([
                screen_id,
                plate,
                plate_name,
                id,
                cell_line,
                strain,
                gene_identifier,
                phenotype_identifier,
                stain,
                stain_target,
                pixel_size_x,
                pixel_size_y
            ])

    # Output results
    plate_results_df = pd.DataFrame(
        plate_results,
        columns=[
            "screen_id",
            "plate_id",
            "plate_name",
            "image_id",
            "cell_line",
            "strain",
            "gene_identifier",
            "phenotype_identifier",
            "stain",
            "stain_target",
            "pixel_size_x",
            "pixel_size_y"
        ]
    )

    return plate_results_df


data_dir = pathlib.Path(
    "/home/parkerhicks/Documents/publicly-available-microscopy-data/IDR/data")

# Load IDR ids
id_file = pathlib.Path(data_dir, "idr_ids.tsv")

id_df = pd.read_csv(id_file, sep="\t")

# Create session
INDEX_PAGE = "https://idr.openmicroscopy.org/webclient/?experimenter=-1"

# create http session
with requests.Session() as session:
    request = requests.Request('GET', INDEX_PAGE)
    prepped = session.prepare_request(request)
    response = session.send(prepped)
    if response.status_code != 200:
        response.raise_for_status()

# Extract summary details for all screens
screen_ids = id_df.query("category=='Screen'").id.tolist()
print(f"There are a total of {len(screen_ids)} screens")

screen_details_df = (
    pd.concat([
       extract_study_info(session=session, screen_id=x) for x in screen_ids
    ], axis="rows")
    .reset_index(drop=True)
)

output_file = pathlib.Path(data_dir, "screen_details.tsv")
screen_details_df.to_csv(output_file, index=False, sep="\t")

plate_info = []
count = 0
for idx, screen in screen_details_df.iterrows():
    screen_id = screen.screen_id
    print(f"Now processing screen: {screen_id}")

    # Pull pertinent details about the screen (plates, wells, channels, cell line, etc.)
    plate_results_df = describe_screen(session, screen_id=screen_id)
    print(plate_results_df)
    print(f"{plate_results_df.shape[0]} images found. Done\n")

    # Combine to create full dataframe
    plate_info.append(plate_results_df)

    # Break here just for quick testing. Will remove
    count += 1
    if count == 3:
        break

all_plate_results_df = pd.concat(plate_info).reset_index(drop=True)

img_screen_index = dict()
for index in screen_details_df.itertuples(index=False):
    screenID = index[19]
    img_type = index[5]
    img_screen_index[screenID] = img_type

all_plate_results_df["imaging_method"] = all_plate_results_df["screen_id"].map(
    img_screen_index)

output_file = pathlib.Path(data_dir, "plate_details_per_screen.tsv")
all_plate_results_df.to_csv(output_file, index=False, sep="\t")
