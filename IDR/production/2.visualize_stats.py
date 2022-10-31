import pathlib

import pandas as pd
from plotnine import *
from sigfig import round


if __name__ == "__main__":

    # Load data
    databank_stats_dir = pathlib.Path("IDR/data/statistics/databank_diversity.parquet")
    databank_stats = pd.read_parquet(databank_stats_dir)

    study_stats_dir = pathlib.Path(
        "IDR/data/statistics/individual_studies_diversity.parquet"
    )
    study_stats = pd.read_parquet(study_stats_dir)

    dodge_text = position_dodge(width=0.9)

    # Round to 3 sigfigs if not 0
    stats_to_round = ["H", "J", "NME", "E", "GC"]
    for stat in stats_to_round:
        databank_stats[stat] = databank_stats[stat].apply(
            lambda x: round(x, sigfigs=3) if x > 0 else x
        )
        study_stats[stat] = study_stats[stat].apply(
            lambda x: round(x, sigfigs=3) if x > 0 else x
        )

    # Set output images directory
    study_imgs_dir = pathlib.Path("IDR/data/statistics/imgs/study_stat_imgs")
    databank_imgs_dir = pathlib.Path("IDR/data/statistics/imgs/databank_stat_imgs")

    atts_to_remove = ["well_id", "plate_id", "Sample", "Organism Part", "Phenotype Identifier", "Oraganism Part"]
    for att in atts_to_remove:
        databank_stats.drop(databank_stats[databank_stats["Attribute"] == att].index, inplace=True)
        study_stats.drop(study_stats[study_stats["Attribute"] == att].index, inplace=True)

    # Plot stats for databank and individual study stats
    stats_to_graph = ["H", "J", "NME", "E", "GC", "S"]
    stat_titles = {"H": "Shannon Index (H')", "J": "Pielou's Evenness (J')", "NME": "Normalized Median Evenness (NME)", "E": "Simpson's Evenness (E)", "GC": "Gini Coefficient (GC)", "S": "Richness (S)"}
    for stat in stats_to_graph:
        databank_stats_sorted = databank_stats.sort_values(by=[stat], ascending=True, na_position='last')
        stats_list = databank_stats_sorted['Attribute'].value_counts().index.tolist()
        databank_plot = (
            ggplot(data=databank_stats_sorted, mapping=aes(x="Attribute", y=stat))
            + geom_bar(na_rm=True, stat="identity", position="dodge")
            + geom_text(
                aes(label=stat),
                position=dodge_text,
                color="gray",
                size=8,
                ha="left",
            )
            + scale_x_discrete(limits=stats_list)
            + ylab(stat_titles[stat])
            + ylim(0, (1.3 * max(databank_stats[stat])))
            + coord_flip()
        )

        studies_plot = (
            ggplot(data=study_stats, mapping=aes(x="Attribute", y=stat))
            + geom_jitter(na_rm=True, stat="identity", position="jitter")
            + theme(axis_text_x=element_text(rotation=90))
            + ylab(stat_titles[stat])
            + ylim(0, max(study_stats[stat]))
        )

        databank_output_file = pathlib.Path(databank_imgs_dir, f"{stat}.png")
        databank_plot.save(databank_output_file, dpi=500)

        studies_output_file = pathlib.Path(study_imgs_dir, f"{stat}.png")
        studies_plot.save(studies_output_file, dpi=500)
