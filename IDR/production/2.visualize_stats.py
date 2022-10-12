import pandas as pd
from plotnine import *
import pathlib
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

    # Plot stats for databank and individual study stats
    stats_to_graph = ["H", "J", "NME", "E", "GC", "S"]
    for stat in stats_to_graph:
        databank_plot = (
            ggplot(data=databank_stats, mapping=aes(x="Attribute", y=stat))
            + geom_col(na_rm=True, stat="identity", position="identity")
            + geom_text(
                aes(label=stat),
                position=dodge_text,
                color="gray",
                size=8,
                ha="left",
            )
            + coord_flip()
        )

        studies_plot = (
            ggplot(data=study_stats, mapping=aes(x="Attribute", y=stat))
            + geom_jitter(na_rm=True, stat="identity", position="jitter")
            + theme(axis_text_x=element_text(rotation=90))
            + ylim(0, max(study_stats[stat]))
        )

        databank_output_file = pathlib.Path(databank_imgs_dir, f"{stat}.png")
        databank_plot.save(databank_output_file)

        studies_output_file = pathlib.Path(study_imgs_dir, f"{stat}.png")
        studies_plot.save(studies_output_file)
