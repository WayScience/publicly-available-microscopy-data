import pathlib
import subprocess
import sys

import pandas as pd
from utils.args import collect_screen_metadata_parser


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
    # Define arguments
    args = collect_screen_metadata_parser().parse_args(sys.argv[1:])

    # Specify where to extract metadata from
    file_type = args.metadata_fileType

    # Get IDR IDs
    print("\nExtracting screen ids from IDR\n")
    get_idr_ids = subprocess.Popen(args=["python3", "IDR/production/utils/get_ids.py"])
    get_idr_ids.wait()

    if file_type == "downloaded_json":
        json_metadata_dir = pathlib.Path("IDR/data/json_metadata")
        if json_metadata_dir.exists() == False:
            # Download JSON metadata
            print(
                "Downloading JSON metadata from IDR API \n WARNING: This process can take multiple days to execute. \n"
            )
            answer = input("Do you wish to download JSON metadata? \n\n y/n:")
            if answer == "y":
                get_json_files = subprocess.Popen(
                    args=[
                        "python3",
                        "IDR/production/metadata_extraction/get_json_files.py",
                    ]
                )
                get_json_files.wait()
            elif answer == "n":
                exit()

        elif json_metadata_dir.exists():
            # Extract metadata from downloaded JSON files
            extract_metadata = subprocess.Popen(
                args=[
                    "python",
                    "IDR/production/metadata_extraction/process_json_metadata.py",
                ]
            )
            extract_metadata.wait()
