#!/usr/bin/env python
# coding: utf-8

# ## Visualize Screen Metadata
# 
# Each screen is a unique experiment. Visualize the diversity of experimental details in IDR screens.
# 
# **Note:** This notebook is WIP

# In[1]:


import pathlib
import re
import pandas as pd
import plotnine as gg


# In[2]:


data_dir = pathlib.Path("data")


# ## Load data

# In[3]:


# Load IDR ids
id_file = pathlib.Path(data_dir, "idr_ids.tsv")

id_df = pd.read_csv(id_file, sep="\t")

print(id_df.shape)
id_df.head(2)


# In[4]:


# Load screen details
screen_file = pathlib.Path(data_dir, "screen_details.tsv")
screen_df = pd.read_csv(screen_file, sep="\t")

print(screen_df.shape)
screen_df.head(2)


# In[5]:


# Load all plates
plate_file = pathlib.Path(data_dir, "plate_details_per_screen.tsv")

plate_df = pd.read_csv(plate_file, sep="\t").merge(screen_df, on="screen_id", how="left")
plate_df.n_wells = plate_df.n_wells.astype(str)

print(plate_df.shape)
plate_df.head(2)


# ## Explore visualizations

# In[6]:


# Number of wells per experiment
print(f"The total number of wells is: {plate_df.n_wells.astype(int).sum():,d}")

mean_num_plates = plate_df.groupby("idr_name")["screen_id"].count().mean()
print(f"The mean number of plates is: {mean_num_plates}")

gg.options.figure_size = (6.4, 6.4)

(
    gg.ggplot(plate_df, gg.aes(x="idr_name", fill="n_wells"))
    + gg.geom_bar()
    + gg.theme_bw()
    + gg.theme(axis_text_y=gg.element_text(size=6))
    + gg.coord_flip()
    + gg.geom_hline(yintercept=mean_num_plates, linetype="dashed")
    + gg.ylab("Number of Plates")
    + gg.xlab("IDR Screen Accession")
    + gg.scale_fill_discrete(name="Plate size")
)


# In[7]:


# Channels per plate
channel_df = (
    plate_df
    .assign(
        split_channels=plate_df.channels.str.split(";")
    )
    .loc[:, ["screen_id", "plate_name", "idr_name", "channels", "split_channels"]]
    .drop_duplicates(subset=["plate_name", "screen_id"])
    .reset_index(drop=True)
)

channel_df.head()


# In[8]:


channel_df.drop_duplicates("screen_id").channels.unique()


# In[9]:


test = channel_df.query("idr_name=='idr0069-caldera-perturbome/screenA'")
test


# In[ ]:





# In[10]:


exclusions = [
    "ch00:",
    "ch01:",
    "ch02:",
    "exp1cam1:",
    "exp2cam2:",
    "exp3cam3:",
    "exp4cam2:"
]

def split_channel_marker(x, exclusions):
    
    remove = [x.startswith(y) for y in exclusions] 
    
    if any(remove):
        prefix_removal = dict(zip(remove, exclusions))[True]
        x = x.removeprefix(prefix_removal)
        
    return x


def convert_to_colon_notation(x):
    try:
        found = re.search(r'\(.*?\)', x).group(0)
        mark = found.strip("()")
        stain = x.replace(found, "").rstrip()
        output = f"{stain}:{mark}"
    except AttributeError:
        output = x

    return output


# In[11]:


def curate_channel(group):
    
    if group[0] == "Not listed":
        return []
    
    group = [x.split(",") for x in group]
    group = [item for sublist in group for item in sublist]
    
    group = [convert_to_colon_notation(item.lstrip().lower()) for item in group]
    
    return [split_channel_marker(x, exclusions) for x in group]


# In[12]:


full_df = channel_df.assign(curated_channel=[curate_channel(x) for x in channel_df.split_channels])
full_df


# In[13]:


full_channel_list_df = full_df.curated_channel.apply(pd.Series).stack().reset_index().loc[:, ["level_0", 0]]
full_channel_list_df.columns = ["original_index", "channel"]

final_df = full_df.merge(full_channel_list_df, how="right", left_index=True, right_on="original_index")
final_df.head()


# In[14]:


split_up_df = final_df.channel.str.split(":").apply(pd.Series).loc[:, [0, 1]]
split_up_df.columns = ["stain", "mark"]
final_full_df = pd.concat([final_df, split_up_df], axis="columns").loc[:, ["screen_id", "plate_name", "idr_name", "channel", "stain", "mark"]]
final_full_df


# In[15]:


channel_count_per_screen_df = final_full_df.groupby(["screen_id", "idr_name", "channel", "stain", "mark"]).plate_name.count().reset_index()
channel_count_per_screen_df = channel_count_per_screen_df.rename(columns={"plate_name": "plate_count"}).sort_values(by=["channel", "plate_count"], ascending=[True, False]).reset_index(drop=True)
channel_count_per_screen_df


# In[16]:


channel_count_per_screen_df.query("idr_name=='idr0080-way-perturbation/screenA'")


# In[17]:


channel_count_per_screen_df.to_csv("data/channel_count_per_screen.tsv", sep="\t", index=False)

