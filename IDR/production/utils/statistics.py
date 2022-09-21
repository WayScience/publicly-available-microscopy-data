import numpy as np
from scipy import ndimage
from numpy import log as ln


def category_frequencies(attribute_elements):
    """Calculates absolute and relative frequencies for unique elements of an image attribute
    Parameters
    ----------
    attribute_elements: dict
        Unique elements of an image attribute as keys and instances of each as values
    Returns
    -------
    rel_freq_dict: dict
        Relative frequencies per unique image attribute element
    abs_freq-list: list
        Instance counts of unique image attribute elements
    """
    total_instances = sum(attribute_elements.values())
    rel_freq_dict = dict()
    abs_freq_list = list()
    for image_attribute in attribute_elements.keys():
        abs_freq_list.append(attribute_elements[image_attribute])
        rel_freq_dict[image_attribute] = (
                attribute_elements[image_attribute] / total_instances
        )
    return rel_freq_dict, abs_freq_list


def h_index(rel_frequencies):
    """Calculates the Shannon Index of a set of unique attribute instances.
    Parameters
    ----------
    rel_frequencies: dict
        Dictionary of relative frequencies for each unique element in an attribute column.
    Returns
    -------
    h: float
        Shannon Index value
    results: list
        List of each -p_iln(p_i) value to use for Normalized Median Evenness statistic.
    """
    results = list()
    for entry in rel_frequencies.values():
        results.append(entry * ln(entry))

    results = np.array(results)
    h = -(sum(results))
    return h, results


def pielou(h, s):
    """Calculates the ratio of the observed Shannon Index to the maximum possible Shannon Index value
    Parameters
    ----------
    h: float
        Observed Shannon Index calculated by h_index()
    s: int
        Observed richness within an image attribute (count of unique entries)
    Returns
    -------
    j: float
        Pielou's evenness (H_obs / H-max)
    """
    j = h / ln(s)
    return j


def norm_median_evenness(h_list):
    """Calculates Normalized Median Evenness (NME) of Shannon Index summation elements (-p*ln(p))
    Parameters
    ----------
    h_list: list
        Individual -p*ln(p) values of the Shannon Index summation
    Returns
    -------
    nme: float
        Ratio of median and max -p*ln(p) values
    """
    # Multiply each value in h_list by -1
    h_values = np.array([-1.0 * h_value for h_value in h_list])

    # Calculate NME
    nme = ndimage.median(h_values) / h_values.max()
    return nme


def gini_coef(absolute_frequencies_list):
    """Calculates the Gini coefficient of inequality across unique elements per image attribute
    Parameters
    ----------
    absolute_frequencies_list: list
        Counts of instances of each unique element of an image attribute
    Returns
    -------
    gc: float
        Measure of inequality in range [0, 1] where 0 is perfect equality and 1 is perfect inequality
    """
    absolute_frequencies = np.array(absolute_frequencies_list)

    # Integrate the Lorenze Curve
    total = 0
    for count, abs_freq in enumerate(absolute_frequencies[:-1], 1):
        total += np.sum(np.abs(abs_freq - absolute_frequencies[count:]))

    gc = total / (len(absolute_frequencies) ** 2 * np.mean(absolute_frequencies))
    return gc


def stats_pipeline(attribute_elements):
    """Pipeline to calculate all pertinant diversity statistics

    Parameters
    ----------
    attribute_elements: dict
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
