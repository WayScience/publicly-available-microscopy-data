import pandas as pd
from plotnine import *
import pathlib
from sigfig import round
import matplotlib.pyplot as plt


if __name__ == "__main__":

    # Load data
    databank_stats_dir = pathlib.Path("IDR/data/statistics/databank_diversity.parquet")
    databank_stats = pd.read_parquet(databank_stats_dir)

    study_stats_dir = pathlib.Path("IDR/data/statistics/individual_studies_diversity.parquet")
    study_stats = pd.read_parquet(study_stats_dir)

    dodge_text = position_dodge(width=0.9)

    attributes_to_round = ["H", "J", "NME", "E", "GC"]
    for attribute in attributes_to_round:
        databank_stats[attribute] = databank_stats[attribute].apply(lambda x: round(x, sigfigs=3) if x>0 else x)
        study_stats[attribute] = study_stats[attribute].apply(lambda x: round(x, sigfigs=3) if x>0 else x)

    study_imgs_dir = pathlib.Path('IDR/data/statistics/imgs/study_stat_imgs')
    databank_imgs_dir = pathlib.Path('IDR/data/statistics/imgs/databank_stat_imgs')

    attributes_to_graph = ["H", "J", "NME", "E", "GC", "S"]
    for attribute in attributes_to_graph:
        p = (ggplot(data=databank_stats, mapping=aes(x="Attribute", y=attribute)) + 
        geom_col(na_rm=True, stat='identity', position='dodge') +
        theme(axis_text_x=element_text(rotation=90)) + 
        geom_text(aes(label=attribute),
             position=dodge_text,
             color='gray', size=8, va='bottom'))

        ps = (ggplot(data=study_stats, mapping=aes(x="Attribute", y=attribute)) + 
        geom_jitter(na_rm=True, stat='identity', position='jitter') +
        theme(axis_text_x=element_text(rotation=90)))

        db_output_file = pathlib.Path(databank_imgs_dir, f"{attribute}.png")
        p.save(db_output_file)

        st_output_file = pathlib.Path(study_imgs_dir, f"{attribute}.png")
        ps.save(st_output_file)
