import pandas as pd
import pathlib
from numpy import log10 as log

def sort_counts(attribute_: str, element_counts_df: pd.DataFrame):
    # Subset 
    att_counts = element_counts_df[element_counts_df["Attribute"] == attribute_]
    att_counts.replace(regex=r"", value="Not listed", inplace=True)

    element_counts_sorted = att_counts.sort_values(by=["Count"], ascending=False, na_position='last').reset_index(drop=True)

    element_counts_sorted["Count_log10"] = element_counts_sorted["Count"].apply(
            lambda x: log(x)
        )
    
    return element_counts_sorted
