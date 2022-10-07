from asyncio import subprocess
import pathlib
import sys
import requests
import subprocess
import pandas as pd
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

    if file_type == "downloaded_json":
        ####### Run get_idr_ids.py first
        json_metadata_dir = pathlib.Path("IDR/data/json_metadata")
        if json_metadata_dir.exists() == False:
            # Download JSON metadata
            print("Downloading JSON metadata from IDR API \n WARNING: This process can take an ungodly amount of time. \n")
            answer = input("Do you wish to download JSON metadata? \n\n y/n:")
            if answer == "y":
                get_json_files = subprocess.Popen(args=["python3", "IDR/production/metadata_extraction/download_jsons/get_json_files.py"])
                get_json_files.wait()
            elif answer == "n":
                exit()

        elif json_metadata_dir.exists() == True:
            # Extract metadata from downloaded JSON files
            subprocess.Popen(args=["python", "IDR/production/metadata_extraction/download_jsons/process_json_metadata.py"])

    elif file_type == "api_access":
            # Download JSON metadata
            print("Downloading JSON metadata from IDR API \n WARNING: This process can take an ungodly amount of time. \n")
            answer = input("Do you wish to download JSON metadata? \n\n y/n:")
            if answer == "y":
                subprocess.Popen(args=["python3", "IDR/production/metadata_extraction/api_access/extract_api_metadata.py"])
            elif answer == "n":
                exit()

    elif file_type == "git_csv":
        print("\nThis metadata extraction workflow in development.\n")

    # # Load IDR ids
    # data_dir = pathlib.Path("IDR/data")
    # id_file = pathlib.Path(data_dir, "idr_ids.tsv")
    # id_df = pd.read_csv(id_file, sep="\t")

    # # Extract summary details for all screens
    # screen_ids = id_df.query("category=='Screen'").id.tolist()
    # screen_details_df = pd.concat(
    #     [extract_study_info(session=session, screen_id=x) for x in screen_ids], axis="rows"
    # ).reset_index(drop=True)

    # output_file = pathlib.Path(data_dir, "screen_details.tsv")
    # screen_details_df.to_csv(output_file, index=False, sep="\t")

    # # Initialize Pool object for threading
    # start = time.time()
    # available_cores = len(os.sched_getaffinity(0))
    # pool = multiprocessing.Pool(processes=available_cores)
    # print(f"\nNow processing {len(screen_ids)} screens with {available_cores} cpu cores.\n")

    # # Collect study_names, imaging method metadata for each screen
    # idr_names_dict = dict()
    # for index in screen_details_df.itertuples(index=False):
    #     screenID = index[19]
    #     idr_name = index[18]
    #     img_type = index[5]
    #     sample = index[0]
    #     idr_names_dict[idr_name] = [screenID, img_type, sample]

    # # Testing studies
    # idr_meta_dict = dict(
    #     (k, idr_names_dict[k])
    #     for k in (
    #         "idr0080-way-perturbation/screenA",
    #         "idr0001-graml-sysgro/screenA",
    #         "idr0069-caldera-perturbome/screenA",
    #     )
    # )

    # test_list = []
    # for key in idr_meta_dict.keys():
    #     test_list.append((key, idr_meta_dict[key]))

    # # Pull & save pertinent details about the screen (plates, wells, channels, cell line, etc.)
    # pool.starmap(collect_metadata, test_list)

    # # Terminate pool processes
    # pool.close()
    # pool.join()

    # print(f"\nMetadata collected. Running cost is {(time.time() - start) / 60:.1f} min.")
