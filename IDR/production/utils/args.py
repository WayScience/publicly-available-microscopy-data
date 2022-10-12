import argparse


help_opt = (
    ("--help", "-h"),
    {"action": "help", "help": "Print this help message and exit"},
)


def collect_screen_metadata_parser():
    parser = argparse.ArgumentParser(
        description="Collecting IDR metadata files per well", add_help=False
    )
    req_args = parser.add_argument_group("Required Arguments")
    req_args.add_argument(
        "-ft",
        dest="metadata_fileType",
        help="REQUIRED: File type for IDR metadata",
        required=True,
        choices=["downloaded_json"],
    )

    return parser
