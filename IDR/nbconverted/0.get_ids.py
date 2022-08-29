import pathlib
import requests
import pandas as pd


def get_id(json):
    """Clean json details in preparation for IDR screen detail storage

    Parameters
    ----------
    json: dict
        One IDR study, with keys storing important pieces of information about the study

    Returns
    -------
    Tuple of ID, name, project title, description, and kind of study.
    """
    id_ = json["@id"]
    name_ = json["Name"]
    details_ = json["Description"]

    if "Screen Description" in details_:
        split_detail = "Screen"
    elif "Study Description" in details_:
        split_detail = "Study"
    elif "Experiment Description" in details_:
        split_detail = "Experiment"

    title_, description_ = details_.split(f"{split_detail} Description\n")
    title_ = title_.replace("Publication Title\n", "").replace("\n", "")
    description_ = description_.replace("\n", "")

    return (id_, name_, title_, description_, split_detail)


output_dir = pathlib.Path(
    "/home/parkerhicks/Documents/publicly-available-microscopy-data/IDR/data")
# Load all screens
INDEX_PAGE = "https://idr.openmicroscopy.org/api/v0/m/screens/"

with requests.Session() as screen_session:
    request = requests.Request('GET', INDEX_PAGE)
    prepped = screen_session.prepare_request(request)
    response = screen_session.send(prepped)
    if response.status_code != 200:
        response.raise_for_status()

screen_info = screen_session.get(INDEX_PAGE).json()
# Load all projects
INDEX_PAGE = "https://idr.openmicroscopy.org/api/v0/m/projects/"

with requests.Session() as project_session:
    request = requests.Request('GET', INDEX_PAGE)
    prepped = project_session.prepare_request(request)
    response = project_session.send(prepped)
    if response.status_code != 200:
        response.raise_for_status()

project_info = project_session.get(INDEX_PAGE).json()
screen_df = pd.DataFrame(
    [get_id(x) for x in screen_info["data"]],
    columns=["id", "name", "title", "description", "category"]
)


project_df = pd.DataFrame(
    [get_id(x) for x in project_info["data"]],
    columns=["id", "name", "title", "description", "category"]
)

id_df = pd.concat([screen_df, project_df], axis="rows").reset_index(drop=True)

# Output to file
output_file = pathlib.Path(output_dir, "idr_ids.tsv")
id_df.to_csv(output_file, index=False, sep="\t")

print(id_df.shape)
