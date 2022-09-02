#!/usr/bin/env python
# coding: utf-8

# # Describe study metadata
# 
# Each screen contains an experiment with different parameters and conditions.
# 
# Extract this information based on ID and save details.

# In[1]:


import random
import pathlib
import requests
import pandas as pd


# In[2]:


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

    MAP_URL = f"https://idr.openmicroscopy.org/webclient/api/annotations/?type=map&screen={screen_id}"

    url = f"{base_url}{screen_url}"
    response = session.get(url).json()
    
    annotations = response["annotations"]
    study_index = [x["ns"] for x in annotations].index('idr.openmicroscopy.org/study/info')
    
    id_ = annotations[study_index]["id"]
    date_ = annotations[study_index]["date"]
    name_ = annotations[study_index]["link"]["parent"]["name"]
    
    details_ = (
        pd.DataFrame({x[0]: x[1] for x in annotations[study_index]["values"]}, index=[0])
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
    pandas.DataFrame() of metadata per plate. This metadata includes details on stains used.
    """
    PLATES_URL = f"https://idr.openmicroscopy.org/webclient/api/plates/?id={screen_id}"
    all_plates = session.get(PLATES_URL).json()["plates"]
    study_plates = {x["id"]: x["name"] for x in all_plates}

    plate_results = []
    for plate in study_plates:
        plate_name = study_plates[plate]
        WELLS_IMAGES_URL = f"https://idr.openmicroscopy.org/webgateway/plate/{plate}/"
        grid = session.get(WELLS_IMAGES_URL).json()
        
        try:
            rowlabels = grid['rowlabels']
            collabels = grid['collabels']

            n_wells_plate = len(rowlabels) * len(collabels)

            pixel_size_x = grid["image_sizes"][0]["x"]
            pixel_size_y = grid["image_sizes"][0]["y"]
        except (ValueError, KeyError):
            plate_results.append([screen_id, plate, plate_name, None, None, None, None, None])
            continue
        
        # Get a random image id:
        rand_image = None
        while rand_image is None:
            rand_row = rowlabels.index(random.choice(rowlabels))
            rand_col = collabels.index(random.choice(collabels))
            cell = grid["grid"][rand_row][rand_col]
            if cell is not None:
                rand_image = cell['id']

        # Find out image details from the random image selected
        MAP_URL = f"https://idr.openmicroscopy.org/webclient/api/annotations/?type=map&image={rand_image}"

        annotations = session.get(MAP_URL).json()["annotations"]
        
        try:
            bulk_index = [x["ns"] for x in annotations].index('openmicroscopy.org/omero/bulk_annotations')
            channels = {x[0]: x[1] for x in annotations[bulk_index]["values"]}["Channels"]
        except (ValueError, KeyError):
            channels = "Not listed"

        try:
            cell_line_index = [x["ns"] for x in annotations].index('openmicroscopy.org/mapr/cell_line')
            cell_line = {x[0]: x[1] for x in annotations[cell_line_index]["values"]}["Cell Line"]
        except (ValueError, KeyError):
            cell_line = "Not listed"
        
        # Build results
        plate_results.append([
            screen_id,
            plate,
            plate_name,
            n_wells_plate,
            cell_line,
            channels,
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
            "n_wells",
            "cell_line",
            "channels",
            "pixel_size_x",
            "pixel_size_y"
        ]
    )
    return plate_results_df


# In[3]:


data_dir = pathlib.Path("data")


# In[4]:


# Load IDR ids
id_file = pathlib.Path(data_dir, "idr_ids.tsv")

id_df = pd.read_csv(id_file, sep="\t")

print(id_df.shape)
id_df.head(10)


# In[5]:


# Create session
INDEX_PAGE = "https://idr.openmicroscopy.org/webclient/?experimenter=-1"

# create http session
with requests.Session() as session:
    request = requests.Request('GET', INDEX_PAGE)
    prepped = session.prepare_request(request)
    response = session.send(prepped)
    if response.status_code != 200:
        response.raise_for_status()


# In[6]:


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

print(screen_details_df.shape)
screen_details_df.head(3)


# In[7]:


plate_info = []
for idx, screen in screen_details_df.iterrows():
    screen_id = screen.screen_id
    print(f"Now processing screen: {screen_id}")
    
    # Pull pertinent details about the screen (plates, wells, channels, cell line, etc.)
    plate_results_df = describe_screen(session, screen_id=screen_id)
    print(f"{plate_results_df.shape[0]} plates found. Done\n")
    
    # Combine to create full dataframe
    plate_info.append(plate_results_df)


# In[8]:


all_plate_results_df = pd.concat(plate_info).reset_index(drop=True)

output_file = pathlib.Path(data_dir, "plate_details_per_screen.tsv")
all_plate_results_df.to_csv(output_file, index=False, sep="\t")

print(all_plate_results_df.shape)
all_plate_results_df.head(10)

"Test change"

