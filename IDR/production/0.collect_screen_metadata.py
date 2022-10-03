import pathlib
import time
import sys
import os
import requests
import pandas as pd
import multiprocessing
import utils.clean_channels as cc
from IDR.production.metadata_extraction.api_access import extract_api_metadata
from IDR.production.metadata_extraction.download_jsons import get_json_files, process_json_metadata
from utils.args import *


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

    # Load IDR ids
    data_dir = pathlib.Path("IDR/data")
    id_file = pathlib.Path(data_dir, "idr_ids.tsv")
    id_df = pd.read_csv(id_file, sep="\t")

    # Create http session
    INDEX_PAGE = "https://idr.openmicroscopy.org/webclient/?experimenter=-1"
    with requests.Session() as session:
        request = requests.Request("GET", INDEX_PAGE)
        prepped = session.prepare_request(request)
        response = session.send(prepped)
        if response.status_code != 200:
            response.raise_for_status()

    # Extract summary details for all screens
    screen_ids = id_df.query("category=='Screen'").id.tolist()
    screen_details_df = pd.concat(
        [extract_study_info(session=session, screen_id=x) for x in screen_ids], axis="rows"
    ).reset_index(drop=True)

    output_file = pathlib.Path(data_dir, "screen_details.tsv")
    screen_details_df.to_csv(output_file, index=False, sep="\t")

    # Initialize Pool object for threading
    start = time.time()
    available_cores = len(os.sched_getaffinity(0))
    pool = multiprocessing.Pool(processes=available_cores)
    print(f"\nNow processing {len(screen_ids)} screens with {available_cores} cpu cores.\n")

    # Collect study_names, imaging method metadata for each screen
    idr_names_dict = dict()
    for index in screen_details_df.itertuples(index=False):
        screenID = index[19]
        idr_name = index[18]
        img_type = index[5]
        sample = index[0]
        idr_names_dict[idr_name] = [screenID, img_type, sample]

    # Testing studies
    idr_meta_dict = dict(
        (k, idr_names_dict[k])
        for k in (
            "idr0080-way-perturbation/screenA",
            "idr0001-graml-sysgro/screenA",
            "idr0069-caldera-perturbome/screenA",
        )
    )

    test_list = []
    for key in idr_meta_dict.keys():
        test_list.append((key, idr_meta_dict[key]))

    # Pull & save pertinent details about the screen (plates, wells, channels, cell line, etc.)
    pool.starmap(collect_metadata, test_list)

    # Terminate pool processes
    pool.close()
    pool.join()

    print(f"\nMetadata collected. Running cost is {(time.time() - start) / 60:.1f} min.")
