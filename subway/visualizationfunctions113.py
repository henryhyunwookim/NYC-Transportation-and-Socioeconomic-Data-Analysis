import os
import pandas as pd
import geopandas as gpd
import ipywidgets as widgets
import matplotlib
import matplotlib.pyplot as plt

from mpl_toolkits.axes_grid1 import make_axes_locatable
from pathlib import Path


def load_cdta_df(folder_name="data"):
    """
    Load the dataframe created by the get_cdta_df function as a geoDataFrame.
    """

    path = os.getcwd() + "\\" + folder_name
    cdta_df = pd.read_csv(f'{path}/subway_cdta.csv', index_col=0)

    # Make the loaded csv file into a geo dataframe.
    cdta_df['geometry'] = gpd.GeoSeries.from_wkt(cdta_df['geometry'])
    cdta_df = gpd.GeoDataFrame(cdta_df, geometry='geometry')
    
    return cdta_df




def plot_total_stations_interactive(entry_or_exit):
    """
    Plot interactive line and bar charts based on pickup and dropoff locations.
    """

    # 0. Load data and set pu_do.
    cdta_df = load_cdta_df(folder_name="data\\subway_df")
    if entry_or_exit == "Entry":
        selectOptions = "Entry"
    elif entry_or_exit == "Exit":
        selectOptions = "Exit"

    # 1. Create a dataframe for station count for each day of the month.
    total_day_df = cdta_df[['borough'] + [col for col in cdta_df.columns if f"{selectOptions}_total_station_count_day" in col]]\
                    .groupby('borough').sum().T
    total_day_df.index = [idx.split("_")[-1] for idx in total_day_df.index]    

    # 2. Create a dataframe for station count for each hour of the day.
    total_hour_df = cdta_df[['borough'] + [col for col in cdta_df.columns if f"{selectOptions}_total_station_count_hour" in col]]\
                    .groupby('borough').sum().T
    total_hour_df.index = [idx.split("_")[-1] for idx in total_hour_df.index]
    
    # 3. Create a dataframe for station count for each weekday.
    total_weekday_df = cdta_df[['borough'] + 
                              [f'{selectOptions}_total_station_count_weekday_Friday',
                              f'{selectOptions}_total_station_count_weekday_Monday',
                              f'{selectOptions}_total_station_count_weekday_Saturday',
                              f'{selectOptions}_total_station_count_weekday_Sunday',
                              f'{selectOptions}_total_station_count_weekday_Thursday',
                              f'{selectOptions}_total_station_count_weekday_Tuesday',
                              f'{selectOptions}_total_station_count_weekday_Wednesday']]\
                        .groupby('borough').sum().T
    total_weekday_df.index = [idx.split("_")[-1] for idx in total_weekday_df.index]
    weekdays = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
    total_weekday_df.index = pd.Categorical(total_weekday_df.index, categories=weekdays, ordered=True)
    total_weekday_df.sort_index(inplace=True)
    
    
    # 4. Create a dataframe for station count for each month of the year; Jan-Jun only.
    total_year_month_df = cdta_df[['borough'] +
                                  [f'{selectOptions}_total_station_count_year_month_2022-01',
                                   f'{selectOptions}_total_station_count_year_month_2022-02',
                                   f'{selectOptions}_total_station_count_year_month_2022-03',
                                   f'{selectOptions}_total_station_count_year_month_2022-04',
                                   f'{selectOptions}_total_station_count_year_month_2022-05',
                                   f'{selectOptions}_total_station_count_year_month_2022-06']]\
                        .groupby('borough').sum().T
    total_year_month_df.index = [idx.split("_")[-1] for idx in total_year_month_df.index]

    # 5. Using the dataframes created above,
    #   create plots for total and average station counts based on different criteria.
    fig, axs = plt.subplots(4, 2, figsize=(20, 20))
    total_day_df.plot(ax=axs[0, 0])
    axs[0, 0].set_title(f"0-0. Total commuter count for each day")

    total_hour_df.plot(ax=axs[1, 0])
    axs[1, 0].set_title(f"1-0. Total commuter count for each hour of the day")

    total_weekday_df.plot(ax=axs[2, 0])
    axs[2, 0].set_title(f"2-0. Total commuter count for each weekday")

    total_year_month_df.plot(ax=axs[3, 0])
    axs[3, 0].set_title(f"3-0. Total commuter count for each month")

    (total_day_df / 6).plot(ax=axs[0, 1])
    axs[0, 1].set_title(f"0-1. Average commuter count for each day ")

    (total_hour_df / 6).plot(ax=axs[1, 1])
    axs[1, 1].set_title(f"1-1. Average commuter count for each hour of the day")

    (total_weekday_df / 6).plot(ax=axs[2, 1])
    axs[2, 1].set_title(f"2-1. Average commuter count for each weekday")

    # 6. Create a bar chart for commuter count for each borough.
    bar_df = total_year_month_df.mean().sort_values(ascending=False)
    ax = axs[3, 1]
    ax.bar(bar_df.index, bar_df.values)
    for i, rect in enumerate(ax.patches):
        # Get X and Y placement of label from rect.
        y_value = rect.get_height()
        x_value = rect.get_x() + rect.get_width() / 2

        # Number of points between bar and label
        space = 0
        # Vertical alignment for positive values
        va = 'bottom'

        # If value of bar is negative: Place label below bar
        if y_value < 0:
            # Invert space to place label below
            space *= -1
            # Vertically align label at top
            va = 'top'

        # Use Y value as label and format number with one decimal place
        label = "{:.3f}%".format(bar_df.values[i] / bar_df.values.sum())

        # Create annotation
        ax.annotate(
            label,                      # Use 'label' as label
            (x_value, y_value),         # Place label at end of the bar
            xytext=(0, space),          # Vertically shift label by 'space'
            textcoords="offset points", # Interpret 'xytext' as offset in points
            ha='center',                # Horizontally center label
            va=va)               
    ax.set_title(f"Average monthly commuter count")
    plt.suptitle(
        "Total and monthly subway commuter counts from Jan-Jun 2022 in NYC",
        fontsize="xx-large",
        fontweight="demibold",
        y=1
        );
    

    

def plot_frequent_stations_interactive(boroname):
    """
    Plot interactive bar charts based on Burough selection.
    """
    # 0. Load Station Borough data
    df = pd.read_csv('data\subway_df\station_boroughs_polygon.csv')
    df = df.drop_duplicates(subset=['stop_name'],keep=False)
    if boroname == "Brooklyn":
        selectOptions = "Brooklyn"
        color = 'red'
    elif boroname == "Bronx":
        selectOptions = "Bronx"
        color = 'blue'
    elif boroname == "Manhattan":
        selectOptions = "Manhattan"
        color = 'green'
    elif boroname == "Queens":
        selectOptions = "Queens"
        color = 'pink'
    elif boroname == "Staten Island":
        selectOptions = "Staten Island"
        color = 'yellow'
        
#     subdf = df[df['BoroName']==selectOptions].drop_duplicates(subset=['stop_name'],keep=False)
    subdf = df[df['BoroName']==selectOptions].sort_values('net_traffic',ascending = False)
    
    x = [i[0] for i in subdf.iloc[:10,17:18].values.tolist()]

    values = [int(i[0]) for i in subdf.iloc[:10,9:10].values.tolist()]
    
    x.reverse()
    values.reverse()
    
    fig = plt.figure(figsize = (15, 10))

    # creating the bar plot
    plt.barh(x, values, color =color, height=0.3)

    plt.xlabel("Station Name")
    plt.ylabel("Net Traffic")
    plt.title(f"Top 10 stations by traffic")
    plt.show()  
    
def plot_on_map_interactive(exclude_manhattan):
    """
    Create a small multiple plot where each choropleth visualizes a station entries and exits variable/stat on a map of NYC.
    """

    cdta_df = load_cdta_df(folder_name="data\\subway_df")
    if exclude_manhattan:
        cdta_df = cdta_df[cdta_df['borough'] != "Manhattan"]

    plot_on_map(cdta_df,  exclude_manhattan)
        


def plot_on_map(df, exclude_manhattan):
    """
    This is a helper function for the plot_on_map_interactive function.
    """
    col_num = 2
    cols = [
        f'net_entries',
        f'net_exits'
    ]
    fig, axes = plt.subplots(1, col_num, figsize=(20, 11))

    axes_idx = []
    for row_idx in range(1):
        for col_idx in range(col_num):
            axes_idx.append((row_idx, col_idx))

    for i,col in enumerate(cols):

        ax = axes[i]
        ax.set_xticks([])
        ax.set_yticks([])
        ax_title = col.split("_", 1)[1].replace("_", " ").capitalize()
        ax.set_title(str(0) + "-" + str(i) + ". " + ax_title)
        divider = make_axes_locatable(ax)
        cax = divider.append_axes("bottom", size="5%", pad=0.2)
        vmin, vmax = df[col].min(), df[col].max()
        df[[col, 'geometry']].plot(
            column=col,
            ax=ax,
            cax=cax,
            legend=True,
            legend_kwds={
                'orientation': 'horizontal'
                },
            vmin=vmin,
            vmax=vmax
        )
        plt.ticklabel_format(scilimits=(0,0))


    title = f"Commuter counts across different community districts(CDTA)"
    if exclude_manhattan:
        title = title + ", excluding Manhattan"
    plt.suptitle(
        title,
        fontsize="xx-large",
        fontweight="demibold",
        y=0.94
        )
    plt.subplots_adjust(top=0.9, hspace=0.1)
   