import pandas as pd
import plotnine as gg
import pathlib

if __name__ == "__main__":

    # Load data
    stats_dir = pathlib.Path("IDR/data/statistics/databank_diversity.parquet.gzip")
    databank_stats = pd.read_parquet(stats_dir)

    print(gg.ggplot(data=databank_stats, mapping=gg.aes(x="Attribute", y="H")) + 
    gg.geom_col(na_rm=True, stat='identity', position='jitter') +
    gg.theme(axis_text_x=gg.element_text(rotation=90))
    )
