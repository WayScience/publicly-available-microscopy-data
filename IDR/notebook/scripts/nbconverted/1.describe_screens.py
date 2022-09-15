#!/usr/bin/env python
# coding: utf-8

# # Describe study metadata
#
# Each screen contains an experiment with different parameters and conditions.
#
# Extract this information based on ID and save details.

# In[8]:


import pathlib
import time
import requests
import pandas as pd
import multiprocessing
import re


# In[9]:


# Define screen metadata extraction functions
def extract_study_info(session, screen_id):
    """Pull metadata info per screen, given screen id

    Parameters
    ----------
    session: Requests.session()
        Requests session providing access to IDR API
    screen_id: int
        ID of the screen data set

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
        "idr.openmicroscopy.org/study/info"
    )

    id_ = annotations[study_index]["id"]
    date_ = annotations[study_index]["date"]
    name_ = annotations[study_index]["link"]["parent"]["name"]

    details_ = pd.DataFrame(
        {x[0]: x[1] for x in annotations[study_index]["values"]}, index=[0]
    ).assign(internal_id=id_, upload_date=date_, idr_name=name_, screen_id=screen_id)

    return details_


def describe_screen(screen_id, sample, imaging_method, study_name):
    """Pull additional metadata info per plate, given screen id

    Parameters
    ----------
    screen_id: int
        ID of the screen data set
    sample: str
        Indicates cell or tissue
    imaging_method: str
        Method used to collect images
    study_name: str
        IDR accession name

    Returns
    -------
    pandas.DataFrame() of the following metadata values:

        screen_id: IDR ID for the screen
        study_name: IDR accession name
        plate_id: IDR ID for each plate
        plate_name: Names given to each plate
        image_id: IDR ID for each image
        sample: Indicates cell or tissue sample
        organism: Genus and species of cells in image
        organism_part: Location of tissue sample
        cell_line: Cell line used in the screen experiment
        strain: Strain of the cell line (if specified)
        gene_identifier: Accession code for the gene being perturbed in a well
        phenotype_identifier: Accession code for the phenotype perturbed in a well
        stain: Set of the stains used in the screen
        stain_target: The target protein or media of the stain
        pixel_size_x: Width of the image
        pixel_size_y: Height of the image
        imaging_method: Method used to collect images (ex: fluorescence microscopy)
    """
    session = requests.Session()

    # Get number of plates per screen and append to a dictionary
    PLATES_URL = f"https://idr.openmicroscopy.org/webclient/api/plates/?id={screen_id}"
    all_plates = session.get(PLATES_URL).json()["plates"]
    study_plates = {x["id"]: x["name"] for x in all_plates}
    # print(f"Number of plates found in screen {screen_id}: ", len(study_plates))

    # Initialize results list
    plate_results = list()

    # Iterate through all plates in the study
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

        # Get image details from each image id for each plate
        for id in imageIDs:
            MAP_URL = f"https://idr.openmicroscopy.org/webclient/api/annotations/?type=map&image={id}"
            annotations = session.get(MAP_URL).json()["annotations"]

            # Get stain and stain target
            try:
                bulk_index = [x["ns"] for x in annotations].index(
                    "openmicroscopy.org/omero/bulk_annotations"
                )
                channels = {x[0]: x[1] for x in annotations[bulk_index]["values"]}[
                    "Channels"
                ]

                # Clean channels value and separate into stain and stain target
                stain = list()
                stain_target = list()
                for channel in channels.split(";"):
                    # For channel entries with 'stain:target' format
                    if bool(re.search(r"([\:])+", channel)):
                        # Separate stain and target
                        st_list = channel.split(":")
                        # Add to respective sets
                        stain.append(st_list[0])
                        stain_target.append(st_list[1])

                    # For channel entries with 'stain (target)' format
                    elif bool(channel[channel.find("(") + 1 : channel.rfind(")")]):
                        # Search for target name within parentheses
                        target_name = channel[
                            channel.find("(") + 1 : channel.rfind(")")
                        ]
                        # Remove parentheses for stain name
                        stain_name = re.sub(r"\([^()]*\)", "", channel)
                        # Remove redundancies (ie. if (target (abbreviation)) present)
                        image_attributes = {"target": target_name, "stain": stain_name}
                        for image_attribute in image_attributes.keys():
                            image_att_name = image_attributes[image_attribute]
                            if bool(
                                image_att_name[
                                    image_att_name.find("(")
                                    + 1 : image_att_name.rfind(")")
                                ]
                            ):
                                image_att_name = re.sub(
                                    r"\([^()]*\)", "", image_att_name
                                )
                                # Add to respective lists
                                if image_attribute == "target":
                                    stain_target.append(target_name)
                                elif image_attribute == "stain":
                                    stain.append(stain_name)

                    else:
                        raise ValueError(
                            "Channels do not adhere to attribute standardization"
                        )

            except (ValueError, KeyError):
                stain = "Not listed"
                stain_target = "Not listed"

            # Get organism
            try:
                organism_index = int(
                    [x["ns"] for x in annotations].index(
                        "openmicroscopy.org/mapr/organism"
                    )
                )
                organism = {x[0]: x[1] for x in annotations[organism_index]["values"]}[
                    "Organism"
                ]
            except (ValueError, KeyError):
                organism = "Not listed"

            # Get organism part
            try:
                organism_part = {x[0]: x[1] for x in annotations[bulk_index]["values"]}[
                    "Oraganism Part"
                ]
            except (ValueError, KeyError):
                organism_part = "Not listed"

            # Get cell line
            try:
                cell_line_index = [x["ns"] for x in annotations].index(
                    "openmicroscopy.org/mapr/cell_line"
                )
                cell_line = {
                    x[0]: x[1] for x in annotations[cell_line_index]["values"]
                }["Cell Line"]
            except (ValueError, KeyError):
                cell_line = "Not listed"

            # Get strain of cell line
            try:
                strain = {x[0]: x[1] for x in annotations[bulk_index]["values"]}[
                    "Strain"
                ]
            except (ValueError, KeyError):
                strain = "Not listed"

            # Get gene identification accession code
            try:
                gene_identifier_index = int(
                    [x["ns"] for x in annotations].index("openmicroscopy.org/mapr/gene")
                )
                gene_identifier = {
                    x[0]: x[1] for x in annotations[gene_identifier_index]["values"]
                }["Gene Identifier"]
            except (ValueError, KeyError):
                gene_identifier = "Not Listed"

            # Get phenotype identification accession code
            try:
                phenotype_identifier_index = int(
                    [x["ns"] for x in annotations].index(
                        "openmicroscopy.org/mapr/phenotype"
                    )
                )
                phenotype_identifier = {
                    x[0]: x[1]
                    for x in annotations[phenotype_identifier_index]["values"]
                }["Phenotype Term Accession"]
            except (ValueError, KeyError):
                phenotype_identifier = "Not Listed"

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
                    stain,
                    stain_target,
                    pixel_size_x,
                    pixel_size_y,
                ]
            )

    # Output results
    plate_results_df = pd.DataFrame(
        plate_results,
        columns=[
            "screen_id",
            "study_name",
            "plate_id",
            "plate_name",
            "image_id",
            "imaging_method",
            "sample",
            "organism",
            "organism_part",
            "cell_line",
            "strain",
            "gene_identifier",
            "phenotype_identifier",
            "stain",
            "stain_target",
            "pixel_size_x",
            "pixel_size_y",
        ],
    )
    return plate_results_df


# In[10]:


def collect_metadata(idr_name, values_list):
    """Data collection and saving pipeline"""
    # Extract names and values
    screen_id = values_list[0]
    imaging_method = values_list[1]
    sample = values_list[2]
    split_name = idr_name.split("/")
    study_name = split_name[0]
    screen_name = split_name[1]

    # Make directories
    data_dir = pathlib.Path("../data/metadata")
    pathlib.Path.mkdir(data_dir, exist_ok=True)
    study_dir = pathlib.Path(data_dir, study_name)
    screen_dir = pathlib.Path(study_dir, screen_name)
    pathlib.Path.mkdir(screen_dir, exist_ok=True)

    # Collect data
    plate_results_df = describe_screen(
        screen_id=screen_id,
        sample=sample,
        imaging_method=imaging_method,
        study_name=study_name,
    )

    # Save data per IDR accession name as .parquet file
    output_file = pathlib.Path(
        screen_dir, f"{study_name}_{screen_name}_{screen_id}.parquet.gzip"
    )
    plate_results_df.to_parquet(output_file, compression="gzip")


# In[11]:


# Load IDR ids
data_dir = pathlib.Path("../data")
id_file = pathlib.Path(data_dir, "idr_ids.tsv")
id_df = pd.read_csv(id_file, sep="\t")


# In[12]:


# Create http session
INDEX_PAGE = "https://idr.openmicroscopy.org/webclient/?experimenter=-1"
with requests.Session() as session:
    request = requests.Request("GET", INDEX_PAGE)
    prepped = session.prepare_request(request)
    response = session.send(prepped)
    if response.status_code != 200:
        response.raise_for_status()


# In[13]:


# Extract summary details for all screens
screen_ids = id_df.query("category=='Screen'").id.tolist()
screen_details_df = pd.concat(
    [extract_study_info(session=session, screen_id=x) for x in screen_ids], axis="rows"
).reset_index(drop=True)

output_file = pathlib.Path(data_dir, "screen_details.tsv")
screen_details_df.to_csv(output_file, index=False, sep="\t")


# In the chunk below, we collect and process a subset of the available IDR screens. The subset contains IDR accession names idr0080, idr0001, and idr0069. The idr0080 dataset was collected by Way et al. (2021) and will be used as a validation set for our statistics pipeline as we already know the image counts and unique attribute counts. The other two datasets are used for prototyping iterative functionalities of metadata collection and statistics.

# In[14]:


# Collect study_names and imaging method metadata for each screen
idr_names_dict = dict()
screen_ids = [
    screen_details_df.at[index, "screen_id"]
    for index in range(len(screen_details_df.index))
]
idr_names = [
    screen_details_df.at[index, "idr_name"]
    for index in range(len(screen_details_df.index))
]
img_types = [
    screen_details_df.at[index, "Imaging Method"]
    for index in range(len(screen_details_df.index))
]
samples = [
    screen_details_df.at[index, "Sample Type"]
    for index in range(len(screen_details_df.index))
]

metadata = list(zip(screen_ids, img_types, samples))
for name, metadata in zip(idr_names, metadata):
    idr_names_dict[name] = metadata

# Subset of studies for prototyping
study_subset = [
    "idr0080-way-perturbation/screenA",
    "idr0001-graml-sysgro/screenA",
    "idr0069-caldera-perturbome/screenA",
]
subset_metadata = [idr_names_dict[study] for study in study_subset]

# Build the iterative object for pool.starmap()
test_list = list(zip(study_subset, subset_metadata))

# Initialize Pool object for threading
start = time.time()
available_cores = len(os.sched_getaffinity(0))
pool = multiprocessing.Pool(processes=available_cores)
print(f"\nNow processing {len(test_list)} screens with {available_cores} cpu cores.\n")

# Pull & save pertinent details about the screen (plates, wells, channels, cell line, etc.)
pool.starmap(collect_metadata, test_list)

# Terminate pool processes
pool.close()
pool.join()

print(f"\nMetadata collected. Running cost is {(time.time()-start)/60:.1f} min.")
