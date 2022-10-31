# Functions for modifying lists


def flatten_list(list_):
    """Concatenates sublists into single list

    Parameters
    ----------
    List_: list
        Metadata from "values" key for each sub-dict in annotations

    Returns
    -------
    List_ sublists concatenated into single list
    """
    return [item for sublist in list_ for item in sublist]


def nested_list_to_dict(nested_list):
    """Converts nested list into dictionary

    Parameters
    ----------
    nested_list: list
        List with dimension=2
        Example: [["Gene Identifier", "gene1"], ["Phenotype Identifier", "phenotype1"]]

    Returns
    -------
    Dictionary with sublist[0] as key and sublist[1] as value
    Example: {"Gene Identifier": "gene1", "Phenotype Identifier": "phenotype1"}

    """
    return {k[0]: k[1:][0] for k in nested_list}


def iterate_through_values(annotations):
    """Converts values from annotations[subdict]["values"] into dictionary for subdict in annotations

    Parameters
    ----------
    annotations: dict
        Dictionary from json.load(metadata_file)["annotations"]

    Returns
    -------
    values_dict: dict
        Output from nested_list_to_dict
    """
    values_list = [sub_dict["values"] for sub_dict in annotations]
    flattened_list = flatten_list(values_list)
    values_dict = nested_list_to_dict(flattened_list)
    return values_dict
