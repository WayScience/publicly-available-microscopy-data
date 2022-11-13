import pandas as pd
import pathlib
from numpy import log10 as log

def sort_counts(attribute_: str, element_counts_df: pd.DataFrame):
    # Subset specified attribute from dataframe
    att_counts = element_counts_df[element_counts_df["Attribute"] == attribute_]
    # Replace empty entries with 'Not listed'
    att_counts.replace(regex=r"", value="Not listed", inplace=True)

    # Sort element counts in decending order
    element_counts_sorted = att_counts.sort_values(by=["Count"], ascending=False, na_position='last').reset_index(drop=True)

    # Compute log_10(Counts) and add to sorted dataframe
    element_counts_sorted["Count_log10"] = element_counts_sorted["Count"].apply(
            lambda x: log(x)
        )
    
    return element_counts_sorted


if __name__ == "__main__":
    # Read counts parquet file as pd.DataFrame
    counts_data = pathlib.Path("IDR/data/statistics/unique_elements_and_counts.parquet")
    counts_df = pd.read_parquet(counts_data)[["Study", "Attribute", "Element", "Count"]]

    # Standardize elements to lowercase 
    counts_df["Element"] = counts_df["Element"].str.lower()

    # Define attributes and file naming schema 
    attributes = {"Phenotype": "Phenotype", "Gene Symbol": "Gene_Symbol", "Compound Name": "Compound", "Cell Line": "Cell_Line", "siRNA Identifier": "siRNA"}
    
    # Iterate through attributes to sort
    for attribute in attributes:
        final_df = sort_counts(attribute_=attribute, element_counts_df=counts_df)

        # Account for alternative phenotype names
        if attribute == "Phenotype":
            alt_df = sort_counts(attribute_="Phenotype Term Name", element_counts_df=counts_df)
            final_df = pd.concat([final_df, alt_df]).reset_index()
        
        # Save attribute counts as individual csv files
        out_path = pathlib.Path("IDR/data/Counts")
        output_file = pathlib.Path(out_path, f"{attributes[attribute]}_Counts.csv")
        final_df.to_csv(output_file)
