import pathlib
import requests
import pandas as pd


def get_id_from_json(json):
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


if __name__ == "__main__":
    data_dir = pathlib.Path("IDR/data")

    # Load all screens
    SCREEN_INDEX_PAGE = "https://idr.openmicroscopy.org/api/v0/m/screens/"

    with requests.Session() as screen_session:
        request = requests.Request("GET", SCREEN_INDEX_PAGE)
        prepped = screen_session.prepare_request(request)
        response = screen_session.send(prepped)
        if response.status_code != 200:
            response.raise_for_status()

    screen_info = screen_session.get(SCREEN_INDEX_PAGE).json()

    # Load all projects
    PROJECT_INDEX_PAGE = "https://idr.openmicroscopy.org/api/v0/m/projects/"

    with requests.Session() as project_session:
        request = requests.Request("GET", PROJECT_INDEX_PAGE)
        prepped = project_session.prepare_request(request)
        response = project_session.send(prepped)
        if response.status_code != 200:
            response.raise_for_status()

    project_info = project_session.get(PROJECT_INDEX_PAGE).json()

    screen_df = pd.DataFrame(
        [get_id_from_json(x) for x in screen_info["data"]],
        columns=["id", "name", "title", "description", "category"],
    )

    project_df = pd.DataFrame(
        [get_id_from_json(x) for x in project_info["data"]],
        columns=["id", "name", "title", "description", "category"],
    )

    id_df = pd.concat([screen_df, project_df], axis="rows").reset_index(drop=True)

    # Extract summary details for all screens
    screen_ids = id_df.query("category=='Screen'").id.tolist()
    screen_details_df = pd.concat(
        [
            extract_study_info(session=requests.Session(), screen_id=x)
            for x in screen_ids
        ],
        axis="rows",
    ).reset_index(drop=True)

    # Output idr_ids as parquet file
    output_file = pathlib.Path(data_dir, "idr_screen_ids.parquet")
    id_df.to_parquet(output_file, index=False)

    # Output screen_details as parquet file
    output_file = pathlib.Path(data_dir, "screen_details.parquet")
    screen_details_df.to_parquet(output_file, index=False)
