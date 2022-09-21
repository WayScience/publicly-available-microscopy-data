import json
import requests


PLATES_IN_SCREEN_URL = f"https://idr.openmicroscopy.org/webclient/api/plates/?id={screen_id}"
WELLS_IN_PLATES_URL = f"https://idr.openmicroscopy.org/webgateway/plate/{plate}/"
MAP_URL = f"https://idr.openmicroscopy.org/webclient/api/annotations/?type=map&well={wellID}"
