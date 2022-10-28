import json
import os
import pathlib

import pandas as pd
import requests
from tqdm import tqdm

if __name__ == "__main__":
    # Initialize session
    session = requests.Session()

    # Create http session
    INDEX_PAGE = "https://idr.openmicroscopy.org/webclient/?experimenter=-1"
    with session:
        request = requests.Request("GET", INDEX_PAGE)
        prepped = session.prepare_request(request)
        response = session.send(prepped)
        if response.status_code != 200:
            response.raise_for_status()

    # Load idr screen ids
    idr_ids_file = pathlib.Path("IDR/data/idr_ids.tsv")
    screen_ids = pd.read_csv(idr_ids_file, sep="\t").id.values.tolist()

    json_metadata_dir = pathlib.Path("IDR/data/json_metadata")
    available_screens = os.listdir(json_metadata_dir)
    available_screens = [int(screen_id) for screen_id in available_screens]

    screen_ids = [id_ for id_ in screen_ids if id_ not in available_screens]

    # Get plate ids
    for screen_id in tqdm(screen_ids):
        PLATES_IN_SCREEN_URL = (
            f"https://idr.openmicroscopy.org/webclient/api/plates/?id={screen_id}"
        )
        all_plates = session.get(PLATES_IN_SCREEN_URL).json()["plates"]
        study_plates = {x["id"]: x["name"] for x in all_plates}

        # Get well ids
        for plate in study_plates:
            WELLS_IN_PLATES_URL = (
                f"https://idr.openmicroscopy.org/webgateway/plate/{plate}"
            )
            wellIDs = list()
            plate_name = study_plates[plate]

            # Access .json file for the plate ID. Contains well ID numbers
            # for each well per plate.
            all_wells = session.get(WELLS_IN_PLATES_URL).json()

            excluded_keys = ["collabels", "rowlabels", "image_sizes"]
            for key in excluded_keys:
                all_wells.pop(key, None)
            for row in range(len(all_wells["grid"])):
                for well in all_wells["grid"][row]:
                    if well is not None:
                        # Append IDs to iterable lists
                        wellIDs.append(well["wellId"])
                    else:
                        pass

            for wellID in wellIDs:
                MAP_URL = f"https://idr.openmicroscopy.org/webclient/api/annotations/?type=map&well={wellID}"

                # Connect to IDR API
                well_metadata = session.get(MAP_URL).json()

                # Make directories for each screen ID and plate ID
                output_dir = pathlib.Path(f"IDR/data/json_metadata/{screen_id}/{plate}")
                pathlib.Path.mkdir(output_dir, exist_ok=True, parents=True)

                # Download the json files
                output_file = pathlib.Path(output_dir, f"{wellID}.json")
                with open(output_file, "w", encoding="utf-8") as file:
                    json.dump(well_metadata, file, ensure_ascii=False, indent=4)
