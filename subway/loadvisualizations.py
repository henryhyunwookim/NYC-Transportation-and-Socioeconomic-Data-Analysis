import os
import pandas as pd
import geopandas as gpd
import ipywidgets as widgets
import matplotlib
import matplotlib.pyplot as plt



def load_cdta_df(folder_name="data"):
    """
    Load the dataframe created by the get_cdta_df function as a geoDataFrame.
    """

    path = os.getcwd() + "\\" + folder_name
    cdta_df = pd.read_csv(f'{path}/subway_cdta.csv', index_col=0)

    # Make the loaded csv file into a geo dataframe.
    cdta_df['geometry'] = gpd.GeoSeries.from_wkt(cdta_df['geometry'])
    cdta_df = gpd.GeoDataFrame(cdta_df, geometry='geometry')
    
    print(f"cdta_df loaded from {path}.")

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
    axs[0, 0].set_title(f"{selectOptions}_total_station_count_day")

    total_hour_df.plot(ax=axs[1, 0])
    axs[1, 0].set_title(f"{selectOptions}_total_station_count_hour")

    total_weekday_df.plot(ax=axs[2, 0])
    axs[2, 0].set_title(f"{selectOptions}_total_station_count_weekday")

    total_year_month_df.plot(ax=axs[3, 0])
    axs[3, 0].set_title(f"{selectOptions}_total_station_count_year_month")

    (total_day_df / 6).plot(ax=axs[0, 1])
    axs[0, 1].set_title(f"{selectOptions}_monthly_average_count_day")

    (total_hour_df / 6).plot(ax=axs[1, 1])
    axs[1, 1].set_title(f"{selectOptions}_monthly_average_count_hour")

    (total_weekday_df / 6).plot(ax=axs[2, 1])
    axs[2, 1].set_title(f"{selectOptions}_monthly_average_count_weekday")

    # 6. Create a bar chart for trip count for each borough.
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
    ax.set_title(f" {selectOptions}_monthly_average_station_count_per_borough")
    plt.suptitle(
        "Total and monthly subway station counts in Jan-Jun 2022 in NYC",
        fontsize="xx-large",
        fontweight="demibold",
        y=1
        );