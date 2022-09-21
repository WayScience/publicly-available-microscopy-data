import pandas as pd
import pathlib
import multiprocessing
import time
from utils.io import walk


def stats_pipeline(attribute_elements):
    """Pipeline to calculate all pertinant diversity statistics

    Parameters
    ----------
    attribtute_elements: dict
        Unique elements of an image attribute as keys and instances of each as values

    Returns
    -------
    s: int
        Richness
    h: float
        Shannon Index
    nme: float
        Normalized Median Evenness
    j: float
        Pielou's Evenness
    gc: float
        Gini coefficient
    """
    # Richness
    s = len(attribute_elements.keys())

    # Shannon Index
    rel_frequencies, abs_frequencies = category_frequencies(
        attribute_elements=attribute_elements
    )
    h, pi_list = h_index(p=rel_frequencies)

    # Calculate Normalized Median Evenness
    nme = norm_median_evenness(pi_list)

    # Calculate Pielou's evenness
    j = pielou(h=h, s=s)

    # Calculate Gini coefficient
    gc = gini_coef(abs_frequencies)

    return s, h, nme, j, gc

