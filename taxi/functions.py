import requests
import bs4
from collections import defaultdict
import os
import dask.dataframe as dd
import geopandas as gpd
import pandas as pd
from zipfile import ZipFile
import matplotlib
import matplotlib.pyplot as plt
import imageio.v2 as imageio
from mpl_toolkits.axes_grid1 import make_axes_locatable
from tabulate import tabulate
import webbrowser
import folium
import warnings
warnings.filterwarnings("ignore")
import seaborn as sns


def load_taxi_trip_data(source_url, folder_name="data"):
    """
    Download yellow and green taxi trip data in the local drive and load them as dataframes.
    source_url = "https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page"
    * NYC Taxi and Limousine Commission (TLC)
    """
    res = requests.get(source_url, timeout=30)
    doc = bs4.BeautifulSoup(res.text, 'html.parser')
    table = doc.find('table') # Find the first table for the current year.

    dataframes = defaultdict(list)
    for a in table.find_all('a'):
        url = a['href']
        filename = url.split("/")[-1]
        taxi_type = filename.split("_")[0]
        month = int(filename.split("-")[1].split(".")[0])
        
        # Only yellow and green taxi trip data will be used due to limited resources.
        # Also, only Jan-Jun 2022 data will be used for the same constraint.
        if (taxi_type in ["yellow", "green"]) and (month < 7):
            path = os.getcwd() + "/" + folder_name + "/" + taxi_type
            if not os.path.exists(path):
                os.makedirs(path)
                print(f"Folder created: {path}")

            file_path = path + "/" + filename
            if not os.path.isfile(file_path):
                res = requests.get(url, allow_redirects=True)
                with open(file_path, 'wb') as file:
                    file.write(res.content)
                print(f"{filename} downloaded in {path}.")
            else:
                print(f"{filename} already exists in {path}.")

            df = dd.read_parquet(path + "/" + filename, ignore_metadata_file=True, split_row_groups=True)
            dataframes[taxi_type].append(df)
    
    return dataframes


def concat_dataframes(dataframes_dict):
    """
    yellow taxi trip data and green taxi trip data mostly share common columns,
    thus it makes sense to concatenate them as a single dataframe.
    """
    yellow_df = dd.multi.concat(dataframes_dict['yellow'])
    yellow_df = yellow_df.rename(columns={"tpep_pickup_datetime":  "pickup_datetime",
                            "tpep_dropoff_datetime": "dropoff_datetime"})

    green_df = dd.multi.concat(dataframes_dict['green'])
    green_df = green_df.rename(columns={"lpep_pickup_datetime":  "pickup_datetime",
                            "lpep_dropoff_datetime": "dropoff_datetime"})

    street_hail_df = dd.multi.concat([yellow_df, green_df])
    
    return street_hail_df


def load_taxi_zones_shp(source_url, folder_name="data", target_filename = "taxi_zones.zip"):
    """
    Download taxi zones data in the local drive and load .shp file as dataframe.
    The taxi zones are roughly based on NYC Department of City Planning’s Neighborhood Tabulation Areas (NTAs).
    Source: https://data.cityofnewyork.us/Transportation/NYC-Taxi-Zones/d3c5-ddgc
    """
    # Download data
    res = requests.get(source_url, timeout=30)
    doc = bs4.BeautifulSoup(res.text, "html.parser")
    for a in doc.find_all("a"):
        url = a["href"]
        if target_filename in url:
            path = os.getcwd() + "/" + folder_name
            if not os.path.exists(path):
                os.makedirs(path)
                print(f"Folder created: {path}")

            file_path = path + "/" + target_filename
            if not os.path.isfile(file_path):
                res = requests.get(url, allow_redirects=True, timeout=30)
                with open(file_path, 'wb') as file:
                    file.write(res.content)
                print(f"{target_filename} downloaded in {path}.")
            else:
                print(f"{target_filename} already exists in {path}.")                
        
    # Load shape file
    with ZipFile(file_path) as zf:
        zf.extractall(folder_name)
    taxi_zones_df = gpd.read_file(path + "\\taxi_zones.shp")
    
    return taxi_zones_df


def map_taxi_zone_to_cdta(nynta2020_df, taxi_zones_df):
    """
    1. Find a match between each taxi zone and NTA based on geometry data.
    2. Map each taxi zone into NTA, and then from NTA to CDTA.
    3. Return updated taxi_zones_df.
    """
    # Create a temporary dictionary and dataframes for mapping purposes.
    cdta_dict = nynta2020_df[["CDTA2020", "CDTAName"]].set_index("CDTA2020").to_dict()["CDTAName"]
    taxi_geo_df = taxi_zones_df[['LocationID', 'geometry']].set_index('LocationID', drop=True)
    nta_geo_df = nynta2020_df[['NTA2020', 'geometry']].set_index('NTA2020', drop=True)

    # Find a match based on the intersection between each taxi zone and NTA.
    threshold = 0.5
    # There are too many matches for a single location when the threshold < 0.5.
    # There are no matches for more taxi zones when the threshold > 0.5.
    idx_dict = {}
    for taxi_idx, taxi_row in taxi_geo_df.iterrows():
        for nta_idx, nta_row in nta_geo_df.iterrows():
            intersection = taxi_row['geometry'].intersection(nta_row['geometry'])
            if (intersection.area > (taxi_row['geometry'].area * threshold)) or\
                (intersection.area > (nta_row['geometry'].area * threshold)):
                if idx_dict.get(taxi_idx, "") != "":
                    if idx_dict[taxi_idx] != nta_idx:
                        existing_nta_idx = idx_dict[taxi_idx]
                        existing_intersection = nta_geo_df.loc[existing_nta_idx]['geometry'].intersection(nta_row['geometry'])
                        if intersection.area > existing_intersection.area:
                            idx_dict[taxi_idx] = nta_idx
                else:
                    idx_dict[taxi_idx] = nta_idx

    # Map each taxi zone to NTA, and then from NTA to CDTA, based on the matches found.
    # Also add the name of each CDTA to the taxi zones dataframe.
    taxi_zones_df['NTA'] = taxi_zones_df.LocationID.apply(lambda x: idx_dict.get(x, ""))
    taxi_zones_df['CDTA'] = taxi_zones_df['NTA'].apply(lambda x: x[:4])
    taxi_zones_df['CDTA_name'] = taxi_zones_df['CDTA'].map(cdta_dict).fillna("").apply(lambda x: " ".join(x.split()[1:]) if "EWR" not in x else x)

    return taxi_zones_df, idx_dict


def split_datetime(df, input_col, col_name='pickup'):
    """
    Split pickup and dropoff datetime into day, hour, weekday and year_month.
    """
    df[col_name+'_day'] = df[input_col].dt.day
    df[col_name+'_hour'] = df[input_col].dt.hour
    df[col_name+'_weekday'] = df[input_col].dt.day_name()
    df[col_name+'_year_month'] = df[input_col].dt.to_period('M')
    print(f'Splitting "{input_col}" into multiple columns (day, hour, weekday, and year_month).')

    return df


def add_features(street_hail_df, zone_borough_dict, taxi_zone_dict):
    """
    Add features to street_hail_df.
    """
    print('Adding "PU_borough" to street_hail_df based on pickup locations.')
    street_hail_df["PU_borough"] = street_hail_df["PULocationID"].map(zone_borough_dict)
    print('Adding "DO_borough" to street_hail_df based on dropoff locations.')
    street_hail_df["DO_borough"] = street_hail_df["DOLocationID"].map(zone_borough_dict)

    print('Adding "PU_CDTA" to street_hail_df based on pickup locations.')
    street_hail_df["PU_CDTA"] = street_hail_df.PULocationID.map(taxi_zone_dict["CDTA"])
    print('Adding "DO_CDTA" to street_hail_df based on dropoff locations.')
    street_hail_df["DO_CDTA"] = street_hail_df.DOLocationID.map(taxi_zone_dict["CDTA"])

    print('Adding "duration" to street_hail_df based on pickup and dropoff datetimes.')
    street_hail_df['duration'] = (street_hail_df['dropoff_datetime'] - street_hail_df['pickup_datetime'])\
                                .apply(lambda x: round(x.total_seconds() / 60, 2))

    street_hail_df = split_datetime(street_hail_df, 'pickup_datetime', col_name='PU')
    street_hail_df = split_datetime(street_hail_df, 'dropoff_datetime', col_name='DO')

    return street_hail_df


def get_cdta_df(street_hail_df, cdta_geo_dict, taxi_zones_df, folder_name="data"):
    """
    Get a dataframe containing statistics about taxi trips on pickup and dropoff CDTAs.
    """
    pu_sum_df = street_hail_df.groupby("PU_CDTA").sum()
    do_sum_df = street_hail_df.groupby("DO_CDTA").sum()
    
    pu_mean_df = street_hail_df.groupby("PU_CDTA").mean()
    do_mean_df = street_hail_df.groupby("DO_CDTA").mean()

    cdta_df = taxi_zones_df.set_index("CDTA")[["CDTA_name", "borough"]].drop_duplicates()
    
    for pu_do in ["PU", "DO"]:
        cdta_df[f'{pu_do}_total_trip_count'] = street_hail_df.groupby(f'{pu_do}_CDTA').count().iloc[:,0]
        distance_duration_df = street_hail_df.groupby(f'{pu_do}_CDTA').sum()[['trip_distance', 'duration']]
        cdta_df[f'{pu_do}_minute_per_mile'] =\
            distance_duration_df['trip_distance'] / distance_duration_df['duration']

        for agg_func, (pu_df, do_df) in zip(["total", "average"], [(pu_sum_df, do_sum_df), (pu_mean_df, do_mean_df)]):
            cdta_df[f"{pu_do}_{agg_func}_passenger_count"] = pu_df['passenger_count']
            cdta_df[f"{pu_do}_{agg_func}_trip_distance (mile)"] = pu_df['trip_distance']
            cdta_df[f"{pu_do}_{agg_func}_fare"] = pu_df['total_amount']
            cdta_df[f"{pu_do}_{agg_func}_congestion_surcharge"] = pu_df['congestion_surcharge']
            cdta_df[f"{pu_do}_{agg_func}_airport_fee"] = pu_df['airport_fee']
            cdta_df[f"{pu_do}_{agg_func}_duration (min)"] = pu_df['duration']
            
            cdta_df[f"{pu_do}_{agg_func}_passenger_count"] = do_df['passenger_count']
            cdta_df[f"{pu_do}_{agg_func}_trip_distance (mile)"] = do_df['trip_distance']
            cdta_df[f"{pu_do}_{agg_func}_fare"] = do_df['total_amount']
            cdta_df[f"{pu_do}_{agg_func}_congestion_surcharge"] = do_df['congestion_surcharge']
            cdta_df[f"{pu_do}_{agg_func}_airport_fee"] = do_df['airport_fee']
            cdta_df[f"{pu_do}_{agg_func}_duration (min)"] = do_df['duration']

    # DFs for PU trip counts
    pu_day_count_df = street_hail_df.groupby(['PU_CDTA', 'PU_day']).count().reset_index()\
        [['PU_CDTA', 'PU_day', "PULocationID"]].pivot(index='PU_CDTA', columns="PU_day", values="PULocationID")
    pu_day_count_df.columns = ["PU_total_trip_count_day_" + str(col) for col in pu_day_count_df.columns]

    pu_hour_count_df = street_hail_df.groupby(['PU_CDTA', 'PU_hour']).count().reset_index()\
        [['PU_CDTA', 'PU_hour', "PULocationID"]].pivot(index='PU_CDTA', columns="PU_hour", values="PULocationID")
    pu_hour_count_df.columns = ["PU_total_trip_count_hour_" + str(col) for col in pu_hour_count_df.columns]

    pu_weekday_count_df = street_hail_df.groupby(['PU_CDTA', 'PU_weekday']).count().reset_index()\
        [['PU_CDTA', 'PU_weekday', "PULocationID"]].pivot(index='PU_CDTA', columns="PU_weekday", values="PULocationID")
    pu_weekday_count_df.columns = ["PU_total_trip_count_" + str(col) for col in pu_weekday_count_df.columns]

    pu_year_month_count_df = street_hail_df.groupby(['PU_CDTA', 'PU_year_month']).count().reset_index()\
        [['PU_CDTA', 'PU_year_month', "PULocationID"]].pivot(index='PU_CDTA', columns="PU_year_month", values="PULocationID")
    pu_year_month_count_df.columns = ["PU_total_trip_count_" + str(col) for col in pu_year_month_count_df.columns]

    # DFs for DO trip counts
    do_day_count_df = street_hail_df.groupby(['DO_CDTA', 'DO_day']).count().reset_index()\
        [['DO_CDTA', 'DO_day', "DOLocationID"]].pivot(index='DO_CDTA', columns="DO_day", values="DOLocationID")
    do_day_count_df.columns = ["DO_total_trip_count_day_" + str(col) for col in do_day_count_df.columns]

    do_hour_count_df = street_hail_df.groupby(['DO_CDTA', 'DO_hour']).count().reset_index()\
        [['DO_CDTA', 'DO_hour', "DOLocationID"]].pivot(index='DO_CDTA', columns="DO_hour", values="DOLocationID")
    do_hour_count_df.columns = ["DO_total_trip_count_hour_" + str(col) for col in do_hour_count_df.columns]

    do_weekday_count_df = street_hail_df.groupby(['DO_CDTA', 'DO_weekday']).count().reset_index()\
        [['DO_CDTA', 'DO_weekday', "DOLocationID"]].pivot(index='DO_CDTA', columns="DO_weekday", values="DOLocationID")
    do_weekday_count_df.columns = ["DO_total_trip_count_" + str(col) for col in do_weekday_count_df.columns]

    do_year_month_count_df = street_hail_df.groupby(['DO_CDTA', 'DO_year_month']).count().reset_index()\
        [['DO_CDTA', 'DO_year_month', "DOLocationID"]].pivot(index='DO_CDTA', columns="DO_year_month", values="DOLocationID")
    do_year_month_count_df.columns = ["DO_total_trip_count_" + str(col) for col in do_year_month_count_df.columns]

    # Concat dataframes
    cdta_df = pd.concat([cdta_df,
                         pu_day_count_df, pu_hour_count_df, pu_weekday_count_df, pu_year_month_count_df,
                         do_day_count_df, do_hour_count_df, do_weekday_count_df, do_year_month_count_df], axis=1)

    # Add geo data for each CDTA
    cdta_df["geometry"] = cdta_df.index.map(cdta_geo_dict)
    cdta_df = gpd.GeoDataFrame(cdta_df).set_geometry("geometry")
    cdta_df = cdta_df.reset_index().rename(columns={"index": "CDTA"})

    if folder_name != None:
        path = os.getcwd() + "/" + folder_name
        if not os.path.exists(path):
            os.makedirs(path)
            print(f"Folder created: {path}")
        cdta_df.to_csv(f'{path}/cdta_df.csv')

        print(f"cdta_df saved in {path}.")
    
    return cdta_df


def get_cdta_df_per_month(street_hail_df, cdta_geo_dict, taxi_zones_df, folder_name="data"):
    """
    Create a dataframe for taxi trips in each month in Jan-Jun 2022.
    """

    # Create a folder if not already exists.
    path = os.getcwd() + "/" + folder_name
    if not os.path.exists(path):
        os.makedirs(path)
        print(f"Folder created: {path}")

    dfs = []
    year_months = []
    for location_type in ["PU", "DO"]:
        col_name = location_type + "_year_month"

        for year_month in street_hail_df[col_name].unique():
            file_path = f'{path}/cdta_df_{location_type}_{year_month}.csv'
            cdta_df = get_cdta_df(street_hail_df[
                street_hail_df[col_name] == str(year_month)
            ], cdta_geo_dict, taxi_zones_df, None)
            cdta_df.to_csv(file_path)
            dfs.append(cdta_df)
            year_months.append(f"{location_type}_{year_month}")

            print(f"cdta_df_{location_type}_{year_month}.csv saved in {path}.")
    
    return dfs, year_months


def load_cdta_df(folder_name="data"):
    """
    Load the dataframe created by the get_cdta_df function as a geoDataFrame.
    """

    path = os.getcwd() + "/" + folder_name
    cdta_df = pd.read_csv(f'{path}/cdta_df.csv', index_col=0)

    # Make the loaded csv file into a geo dataframe.
    cdta_df['geometry'] = gpd.GeoSeries.from_wkt(cdta_df['geometry'])
    cdta_df = gpd.GeoDataFrame(cdta_df, geometry='geometry')
    
    print(f"cdta_df loaded from {path}.")

    return cdta_df


def load_cdta_df_per_month(folder_name="data",\
    year_month_list = ['2022-01', '2022-02', '2022-03', '2022-04', '2022-05', '2022-06']):
    """
    Load the dataframes created by the get_cdta_df_per_month function as geoDataFrames.
    """

    path = os.getcwd() + "/" + folder_name

    dfs = []
    year_months = []
    for location_type in ["PU", "DO"]:
        for year_month in year_month_list:
            file_path = f'{path}/cdta_df_{location_type}_{year_month}.csv'
            dfs.append(pd.read_csv(file_path, index_col=0))
            year_months.append(f"{location_type}_{year_month}")

            print(f"cdta_df_{location_type}_{year_month}.csv loaded from {path}.")
        
    return dfs, year_months


def plot_total_trips_interactive(pickup_or_dropoff):
    """
    Plot interactive line and bar charts based on pickup and dropoff locations.
    """

    # 0. Load data and set pu_do.
    cdta_df = load_cdta_df(folder_name="data\\cdta_df")
    if pickup_or_dropoff == "Pickup":
        pu_do = "PU"
    elif pickup_or_dropoff == "Dropoff":
        pu_do = "DO"

    # 1. Create a dataframe for trip count for each day of the month.
    total_day_df = cdta_df[['borough'] + [col for col in cdta_df.columns if f"{pu_do}_total_trip_count_day" in col]]\
                    .groupby('borough').sum().T
    total_day_df.index = [idx.split("_")[-1] for idx in total_day_df.index]

    # 2. Create a dataframe for trip count for each hour of the day.
    total_hour_df = cdta_df[['borough'] + [col for col in cdta_df.columns if f"{pu_do}_total_trip_count_hour" in col]]\
                    .groupby('borough').sum().T
    total_hour_df.index = [idx.split("_")[-1] for idx in total_hour_df.index]

    # 3. Create a dataframe for trip count for each weekday.
    total_weekday_df = cdta_df[['borough'] + 
                              [f'{pu_do}_total_trip_count_Friday',
                              f'{pu_do}_total_trip_count_Monday',
                              f'{pu_do}_total_trip_count_Saturday',
                              f'{pu_do}_total_trip_count_Sunday',
                              f'{pu_do}_total_trip_count_Thursday',
                              f'{pu_do}_total_trip_count_Tuesday',
                              f'{pu_do}_total_trip_count_Wednesday']]\
                        .groupby('borough').sum().T
    total_weekday_df.index = [idx.split("_")[-1] for idx in total_weekday_df.index]
    weekdays = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
    total_weekday_df.index = pd.Categorical(total_weekday_df.index, categories=weekdays, ordered=True)
    total_weekday_df.sort_index(inplace=True)
    
    # 4. Create a dataframe for trip count for each month of the year; Jan-Jun only.
    total_year_month_df = cdta_df[['borough'] +
                                    [f'{pu_do}_total_trip_count_2022-01',
                                    f'{pu_do}_total_trip_count_2022-02',
                                    f'{pu_do}_total_trip_count_2022-03',
                                    f'{pu_do}_total_trip_count_2022-04',
                                    f'{pu_do}_total_trip_count_2022-05',
                                    f'{pu_do}_total_trip_count_2022-06']]\
                        .groupby('borough').sum().T
    total_year_month_df.index = [idx.split("_")[-1] for idx in total_year_month_df.index]

    # 5. Using the dataframes created above,
    #   create plots for total and average trip counts based on different criteria.
    fig, axs = plt.subplots(4, 2, figsize=(20, 15))
    fig.tight_layout(pad=5)
    total_day_df.plot(ax=axs[0, 0])
    axs[0, 0].set_title(f"0-0. Total trip count for each day of the month")

    total_hour_df.plot(ax=axs[1, 0])
    axs[1, 0].set_title(f"1-0. Total trip count for each hour of the day")

    total_weekday_df.plot(ax=axs[2, 0])
    axs[2, 0].set_title(f"2-0. Total trip count for each weekday")

    total_year_month_df.plot(ax=axs[3, 0])
    axs[3, 0].set_title(f"3-0. Total trip count for each month")
    
    (total_day_df / 6).plot(ax=axs[0, 1])
    axs[0, 1].set_title(f"0-1. Monthly average trip count for each day of the month")

    (total_hour_df / 6).plot(ax=axs[1, 1])
    axs[1, 1].set_title(f"1-1. Monthly average trip count for each hour of the day")

    (total_weekday_df / 6).plot(ax=axs[2, 1])
    axs[2, 1].set_title(f"2-1. Monthly average trip count for each weekday")
    
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
    ax.set_title(f"3-1. Monthly average trip count for each borough")
    plt.suptitle(
        f"Total and monthly taxi trip counts based on {pickup_or_dropoff.lower()} in Jan-Jun 2022",
        fontsize="xx-large",
        fontweight="demibold",
        y=1
        );


def plot_trips_per_month_interactive(pickup_or_dropoff):
    """
    Create plots similar to what the plot_total_trips_interactive function produces,
    but for each month in 2022.
    """

    if pickup_or_dropoff == "Pickup":
        pu_do = "PU"
    elif pickup_or_dropoff == "Dropoff":
        pu_do = "DO"
    dfs, year_months = load_cdta_df_per_month(folder_name="data\\cdta_df",
                        year_month_list = ['2022-01', '2022-02', '2022-03', '2022-04', '2022-05', '2022-06'])
    plot_trips_per_month(dfs, year_months, pu_do=pu_do)


def plot_total_trips(cdta_df, pu_do, single_month, year_month, save_png):
    """
    This is a helper function for the plot_trips_per_month function.
    The plots created would be similar to what plot_total_trips_interactive creates.
    """

    # 1
    total_day_df = cdta_df[['borough'] + [col for col in cdta_df.columns if f"{pu_do}_total_trip_count_day" in col]]\
                    .groupby('borough').sum().T
    total_day_df.index = [idx.split("_")[-1] for idx in total_day_df.index]

    # 2
    total_hour_df = cdta_df[['borough'] + [col for col in cdta_df.columns if f"{pu_do}_total_trip_count_hour" in col]]\
                    .groupby('borough').sum().T
    total_hour_df.index = [idx.split("_")[-1] for idx in total_hour_df.index]

    # 3
    total_weekday_df = cdta_df[['borough'] + 
                              [f'{pu_do}_total_trip_count_Friday',
                              f'{pu_do}_total_trip_count_Monday',
                              f'{pu_do}_total_trip_count_Saturday',
                              f'{pu_do}_total_trip_count_Sunday',
                              f'{pu_do}_total_trip_count_Thursday',
                              f'{pu_do}_total_trip_count_Tuesday',
                              f'{pu_do}_total_trip_count_Wednesday']]\
                        .groupby('borough').sum().T
    total_weekday_df.index = [idx.split("_")[-1] for idx in total_weekday_df.index]
    weekdays = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
    total_weekday_df.index = pd.Categorical(total_weekday_df.index, categories=weekdays, ordered=True)
    total_weekday_df.sort_index(inplace=True)
    
    year_month_without_pu_do = year_month.split("_")[1]
    if single_month:
        fig, axs = plt.subplots(3, 2, figsize=(15, 12))

        total_day_df.plot(ax=axs[0, 0])
        axs[0, 0].set_title(f"0-0. Total trip count for each day")

        total_hour_df.plot(ax=axs[1, 0])
        axs[1, 0].set_title(f"1-0. Total trip count for each hour of the day")

        total_weekday_df.plot(ax=axs[2, 0])
        axs[2, 0].set_title(f"2-0. Total trip count for each weekday")
        
        (total_day_df / 6).plot(ax=axs[0, 1])
        axs[0, 1].set_title(f"0-1. Average trip count for each day")

        (total_hour_df / 6).plot(ax=axs[1, 1])
        axs[1, 1].set_title(f"1-1. Average trip count for each hour of the day")

        (total_weekday_df / 6).plot(ax=axs[2, 1])
        axs[2, 1].set_title(f"2-1. Average trip count for each weekday")
        
    else:
        # 4
        total_year_month_df = cdta_df[['borough'] +
                                      [f'{pu_do}_total_trip_count_2022-01',
                                       f'{pu_do}_total_trip_count_2022-02',
                                       f'{pu_do}_total_trip_count_2022-03',
                                       f'{pu_do}_total_trip_count_2022-04',
                                       f'{pu_do}_total_trip_count_2022-05',
                                       f'{pu_do}_total_trip_count_2022-06']]\
                            .groupby('borough').sum().T
        total_year_month_df.index = [idx.split("_")[-1] for idx in total_year_month_df.index]

        fig, axs = plt.subplots(4, 2, figsize=(20, 20))
        total_day_df.plot(ax=axs[0, 0])
        axs[0, 0].set_title(f"0-0. Total trip count for each day of the month")

        total_hour_df.plot(ax=axs[1, 0])
        axs[1, 0].set_title(f"1-0. Total trip count for each hour of the day")

        total_weekday_df.plot(ax=axs[2, 0])
        axs[2, 0].set_title(f"2-0. Total trip count for each weekday")

        total_year_month_df.plot(ax=axs[3, 0])
        axs[3, 0].set_title(f"3-0. Total trip count for each month")
        
        (total_day_df / 6).plot(ax=axs[0, 1])
        axs[0, 1].set_title(f"0-1. Monthly average trip count for each day of the month")

        (total_hour_df / 6).plot(ax=axs[1, 1])
        axs[1, 1].set_title(f"1-1. Monthly average trip count for each hour of the day")

        (total_weekday_df / 6).plot(ax=axs[2, 1])
        axs[2, 1].set_title(f"2-1. Monthly average trip count for each weekday")
        
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
        ax.set_title(f"3-1. Monthly average trip count for each borough");

    pickup_or_dropoff = "pickup"
    if pu_do == "DO":
        pickup_or_dropoff = "dropoff"
    plt.suptitle(
        f"Total and monthly taxi trip counts based on {pickup_or_dropoff} in {year_month_without_pu_do}",
        fontsize="xx-large",
        fontweight="demibold",
        y=0.96
        )       
        
    if year_month == None:
        year_month = "Jan-Jun 2022"
    if save_png:
        matplotlib.use('Agg')
        path = os.getcwd() + "/" + "data\\png"
        if not os.path.exists(path):
            os.makedirs(path)
        filepath = f'{path}/{year_month}.png'
        chart = fig.get_figure()
        chart.savefig(filepath, dpi=300)
        print(f"{year_month}.png saved in {path}.")
        plt.close(fig)


def plot_trips_per_month(dfs, year_months, pu_do):
    """
    This is a helper function for the plot_trips_per_month_interactive function.
    """

    if pu_do == "PU":
        dfs = dfs[:int(len(dfs)/2)]
        year_months = year_months[:int(len(year_months)/2)]
    elif pu_do == "DO":
        dfs = dfs[int(len(dfs)/2):]
        year_months = year_months[int(len(year_months)/2):]
        
    for df, year_month in zip(dfs, year_months):
        plot_total_trips(df, pu_do=pu_do, single_month=True, year_month=year_month, save_png=True)
        
    # Create GIF using PNG files.
    print("Getting a GIF file using the PNG files.")
    images = []
    path = os.getcwd() + "/" + "data\\png"
    for file_name in sorted(os.listdir(path)):
        if (file_name.endswith(".png")) and (pu_do in file_name):
            file_path = os.path.join(path, file_name)
            images.append(imageio.imread(file_path))
    imageio.mimsave(path+f'\\{pu_do}_trip_counts_per_month.gif', images, duration=2)
    print(f"{pu_do}_trip_counts_per_month.gif saved in {path}.")


def plot_on_map_interactive(exclude_manhattan, use_fewer_cols):
    """
    Create a small multiple where each choropleth visualizes a taxi trip variable/stat on a map of NYC.
    """

    cdta_df = load_cdta_df(folder_name="data\\cdta_df")
    if exclude_manhattan:
        cdta_df = cdta_df[cdta_df['borough'] != "Manhattan"]
    
    for pu_do in ["PU", "DO"]:
        plot_on_map(cdta_df, pu_do, exclude_manhattan, use_fewer_cols)


def plot_on_map(df, pu_do, exclude_manhattan, use_fewer_cols):
    """
    This is a helper function for the plot_on_map_interactive function.
    """
    if not use_fewer_cols:
        col_num = 7
        cols = [
            f'{pu_do}_total_passenger_count',
            f'{pu_do}_total_fare',
            f'{pu_do}_total_congestion_surcharge',
            f'{pu_do}_total_airport_fee',
            f'{pu_do}_total_duration (min)',
            f'{pu_do}_total_trip_distance (mile)',
            f'{pu_do}_total_trip_count',
            f'{pu_do}_average_passenger_count',
            f'{pu_do}_average_fare',
            f'{pu_do}_average_congestion_surcharge',
            f'{pu_do}_average_airport_fee',
            f'{pu_do}_average_duration (min)',
            f'{pu_do}_average_trip_distance (mile)',
            f'{pu_do}_minute_per_mile'
        ]
        fig, axes = plt.subplots(2, col_num, figsize=(30, 9.5))

    else:
        col_num = 4
        cols = [
            f'{pu_do}_total_trip_count',
            f'{pu_do}_average_fare',
            f'{pu_do}_average_congestion_surcharge',
            f'{pu_do}_average_duration (min)',
            f'{pu_do}_minute_per_mile',
            f'{pu_do}_average_passenger_count',
            f'{pu_do}_average_airport_fee',
            f'{pu_do}_average_trip_distance (mile)'
        ]
        fig, axes = plt.subplots(2, col_num, figsize=(20, 11))


    axes_idx = []
    for row_idx in range(2):
        for col_idx in range(col_num):
            axes_idx.append((row_idx, col_idx))

    for i, col in enumerate(cols):
        ax = axes[axes_idx[i]]
        ax.set_xticks([])
        ax.set_yticks([])
        ax_title = col.split("_", 1)[1].replace("_", " ").capitalize()
        ax.set_title(str(axes_idx[i][0]) + "-" + str(axes_idx[i][1]) + ". " + ax_title)
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

    pickup_or_dropoff = "pickup"
    if pu_do == "DO":
        pickup_or_dropoff = "dropoff"
    title = f"Taxi trips across different community districts based on {pickup_or_dropoff} locations"
    if exclude_manhattan:
        title = title + ", excluding Manhattan"
    plt.suptitle(
        title,
        fontsize="xx-large",
        fontweight="demibold",
        y=0.94
        )
    plt.subplots_adjust(top=0.9, hspace=0.1)


def print_top_table(df, col, top_n=10, excluding_manhattan=False):
    """
    This is a helper function for the plot_taxi_socio_interactive function.
    It outputs top N community districts (or CDTAs) in a neat tabular format.
    """

    top_cdta_df = df[['CDTA', 'CDTA_name', 'borough', col]].sort_values(col, ascending=False)[:top_n].reset_index(drop=True)
    top_cdta_df["Rank"] = [idx+1 for idx in top_cdta_df.index]
    top_cdta_df = top_cdta_df[["Rank", "borough", "CDTA", "CDTA_name", col]]
    top_cdta_df = top_cdta_df.rename(columns={"borough": "Borough"}).fillna("Not available")
    if excluding_manhattan:
        print(f"\t<< Top {top_n} CDTAs based on {col}, excluding Manhattan >>")
    else:
        print(f"\t<< Top {top_n} CDTAs based on {col} >>")
    print(tabulate(top_cdta_df,
                    headers= top_cdta_df.columns,
                    showindex=False,
                    tablefmt='simple_grid',
                    floatfmt=',.7g'))
    print("----------------------------------------------------------------------------------------------------------")


def get_socio_df(cdta_df):
    """
    Load socioeconomic data for each community district and create a dataframe out of it
    so that each socioeconomic indicator for each community district can be plotted on a map.
    """
    ori_socio_df = pd.read_csv(os.path.dirname(os.getcwd()) + "\\socio\\data\\socioecoomic.csv", index_col=0)
    socio_df = ori_socio_df.copy()

    socio_df["Community District"] = socio_df["Community District"].apply(lambda x: x.replace(" ", ""))
    socio_df = socio_df.rename(columns={" Indicator Description": "Indicator Description"})

    # Only select the most relevant columns from the dataframe,
    # and pivot each indicator into a column for each community district.
    socio_cols = [
        'Population',
        'Disabled population',
        'Foreign-born population',
        'Population aged 65+',
        'Median household income (2021$)',
        'Poverty rate',
        'Labor force participation rate',
        'Population aged 25+ without a high school diploma',
        'Unemployment rate',
        'Severely rent-burdened households',
        'Homeownership rate',
        'Severe crowding rate (% of renter households)',
        'Population density (1,000 persons per square mile)',
        'Car-free commute (% of commuters)',
        'Mean travel time to work (minutes)',
        'Serious crime rate (per 1,000 residents)']
    socio_df = socio_df[socio_df["Indicator"].isin(socio_cols)][["Community District", "Indicator", "2019"]]
    socio_df = socio_df.pivot(index="Community District", columns="Indicator", values="2019").reset_index()

    # Add borough, geometry, and centroid data for each community district.
    socio_df["borough"] = socio_df['Community District'].map(
        cdta_df[["CDTA", "borough"]].set_index("CDTA").to_dict()["borough"]
        )
    socio_df['geometry'] = socio_df['Community District'].map(
        cdta_df[["CDTA", "geometry"]].set_index("CDTA").to_dict()["geometry"]
        )
    socio_df = socio_df.set_geometry("geometry")    
    socio_df["centroid"] = socio_df.centroid
    
    # Convert string values into float and remove index name that is not necessary.
    socio_df[socio_cols] = socio_df[socio_cols].applymap(lambda x: float(x.strip('$').strip('%').replace(',', '')))
    socio_df = socio_df.rename_axis(None, axis=1)

    return socio_df


def plot_socio_on_map():
    """
    Similar to the plot_on_map_interactive function, it creates a small multiple where
    each choropleth visualizes a socioeconomic variable/stat on a map of NYC.
    """

    cdta_df = load_cdta_df(folder_name="data\\cdta_df").set_geometry("geometry")
    socio_df = get_socio_df(cdta_df).set_geometry("geometry")

    fig, axes = plt.subplots(4, 4, figsize=(22, 20))
    axes_idx = []
    for row_idx in range(4):
        for col_idx in range(4):
            axes_idx.append((row_idx, col_idx))

    socio_cols = [
        'Population',
        'Disabled population',
        'Foreign-born population',
        'Population aged 65+',
        
        'Population aged 25+ without a high school diploma',
        'Unemployment rate',
        'Poverty rate',
        'Severely rent-burdened households',
        
        'Median household income (2021$)',
        'Labor force participation rate',
        'Severe crowding rate (% of renter households)',
        'Homeownership rate',
        
        'Car-free commute (% of commuters)',
        'Mean travel time to work (minutes)',
        'Population density (1,000 persons per square mile)',
        'Serious crime rate (per 1,000 residents)']

    socio_df = socio_df[socio_cols + ['geometry']]
    for i, col in enumerate(socio_cols):
        ax = axes[axes_idx[i]]
        ax.set_xticks([])
        ax.set_yticks([])
        ax.set_title(str(axes_idx[i][0]) + "-" + str(axes_idx[i][1]) + ". " + col)
        divider = make_axes_locatable(ax)
        cax = divider.append_axes("bottom", size="5%", pad=0.2)
        vmin, vmax = socio_df[col].min(), socio_df[col].max()
        socio_df[[col, 'geometry']].plot(
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

    plt.suptitle(
        "Socioeconomic indicators across different community districts",
        fontsize="xx-large",
        fontweight="demibold",
        y=0.91
        );


def plot_taxi_socio_interactive(taxi_col, socio_col, top_n):
    """
    Plot a taxi trip variable and a socioeconomic variable on the same map,
    with details about top N community districts (or CDTAs) in tables.
    """

    cdta_df = load_cdta_df(folder_name="data\\cdta_df").set_geometry("geometry")
    socio_df = get_socio_df(cdta_df).set_geometry("geometry")

    # Combine the PU and DO columns by averaging the values.
    print("Combining the PU and DO columns by averaging the values in cdta_df.")
    taxi_cols = [
    'total_trip_count', 'PU_minute_per_mile', 'DO_minute_per_mile',
    'total_passenger_count', 'total_trip_distance (mile)',
    'total_fare', 'total_congestion_surcharge', 'total_airport_fee', 'total_duration (min)',
    'average_passenger_count', 'average_trip_distance (mile)', 'average_fare',
    'average_congestion_surcharge', 'average_airport_fee', 'average_duration (min)']

    socio_cols = [
            'Population',
            'Disabled population',
            'Foreign-born population',
            'Population aged 65+',
            'Median household income (2021$)',
            'Poverty rate',
            'Labor force participation rate',
            'Population aged 25+ without a high school diploma',
            'Unemployment rate',
            'Severely rent-burdened households',
            'Homeownership rate',
            'Severe crowding rate (% of renter households)',
            'Population density (1,000 persons per square mile)',
            'Car-free commute (% of commuters)',
            'Mean travel time to work (minutes)',
            'Serious crime rate (per 1,000 residents)']

    print("Combining the PU and DO columns, except minute per mile, by averaging the values in cdta_df.")
    for col in taxi_cols:
        if "minute_per_mile" not in col:
            try:
                cdta_df[col] = (cdta_df["PU_"+col] + cdta_df["DO_"+col]) / 2
                cdta_df.drop(["PU_"+col, "DO_"+col], axis=1, inplace=True)
            except KeyError:
                pass

    fig, ax = plt.subplots(figsize=(8,8))
    divider = make_axes_locatable(ax)
    taxi_vmin, taxi_vmax = cdta_df[taxi_col].min(), cdta_df[taxi_col].max()
    socio_vmin, socio_vmax = socio_df[socio_col].min(), socio_df[socio_col].max()

    cdta_df[['CDTA', 'geometry']].plot(column="CDTA", ax=ax, cax=divider.append_axes("bottom", size="0%", pad=0))

    cdta_df[[taxi_col, 'geometry']].plot(
        column=taxi_col,
        ax=ax,
        cax=divider.append_axes("top", size="5%", pad=0.5),
        legend=True,
        legend_kwds={
            'label': taxi_col,
            'orientation': 'horizontal'
            },
        vmin=taxi_vmin,
        vmax=taxi_vmax,
        cmap='Blues'
    )

    socio_df['centroid'] = socio_df.centroid
    socio_df = socio_df.set_geometry('centroid')
    socio_df[[socio_col, 'centroid']].plot(
        column=socio_col,
        ax=ax,
        cax=divider.append_axes("bottom", size="5%", pad=0.5),
        legend=True,
        legend_kwds={
            'label': socio_col,
            'orientation': 'horizontal'
            },
        vmin=socio_vmin,
        vmax=socio_vmax,
        cmap='Reds',
        marker='.',
        markersize= ((socio_df[socio_col] - socio_df[socio_col].mean()) / socio_vmax) * 2000,
    )

    ax.set_xticks([])
    ax.set_yticks([])
    plt.ticklabel_format(scilimits=(0,0))

    # Print out top 10 CDTAs/CDs for taxi_col and socio_col.
    merged_df = pd.merge(
        cdta_df,
        socio_df.rename(columns={"Community District": "CDTA"}).drop("borough", axis=1),
        on='CDTA',
        how='outer')[taxi_cols + socio_cols + ["CDTA", "CDTA_name", "borough"]]
    print_top_table(merged_df, taxi_col, top_n=top_n)
    print_top_table(merged_df, socio_col, top_n=top_n)


def create_heatmap(variable, top_n, show_highly_correlated_varaibles):
    """
    Create a heatmap to see how taxi trips and socioeconomic statistics are or are not correlated.
    """

    cdta_df = load_cdta_df(folder_name="data\\cdta_df").set_geometry("geometry")
    socio_df = get_socio_df(cdta_df).set_geometry("geometry")

    taxi_cols = [
    'total_trip_count', 'PU_minute_per_mile', 'DO_minute_per_mile',
    'total_passenger_count', 'total_trip_distance (mile)',
    'total_fare', 'total_congestion_surcharge', 'total_airport_fee', 'total_duration (min)',
    'average_passenger_count', 'average_trip_distance (mile)', 'average_fare',
    'average_congestion_surcharge', 'average_airport_fee', 'average_duration (min)']

    print("Combining the PU and DO columns, except minute per mile, by averaging the values in cdta_df.")
    for col in taxi_cols:
        if "minute_per_mile" not in col:
            try:
                cdta_df[col] = (cdta_df["PU_"+col] + cdta_df["DO_"+col]) / 2
                cdta_df.drop(["PU_"+col, "DO_"+col], axis=1, inplace=True)
            except KeyError:
                pass

    socio_cols = [
            'Population',
            'Disabled population',
            'Foreign-born population',
            'Population aged 65+',
            'Median household income (2021$)',
            'Poverty rate',
            'Labor force participation rate',
            'Population aged 25+ without a high school diploma',
            'Unemployment rate',
            'Severely rent-burdened households',
            'Homeownership rate',
            'Severe crowding rate (% of renter households)',
            'Population density (1,000 persons per square mile)',
            'Car-free commute (% of commuters)',
            'Mean travel time to work (minutes)',
            'Serious crime rate (per 1,000 residents)']

    merged_df = pd.merge(cdta_df, socio_df.rename(columns={"Community District": "CDTA"}), on='CDTA', how='right')[taxi_cols+socio_cols]

    fig, ax = plt.subplots(figsize=(20,20))
    sns.heatmap(
        merged_df.corr(),
        annot=True
    )
    # ax.set_title("Correlations among taxi trip and socioeconomic variables")
    plt.suptitle(
        "Correlations among taxi trip and socioeconomic variables",
        fontsize="xx-large",
        fontweight="demibold",
        y=0.9
        )

    # Create a dataframe with columns with the highest absolute correlation coefficients.
    top_df = merged_df.corr()
    absolute_corr_col = "abs_"+variable
    top_df[absolute_corr_col] = top_df[variable].apply(lambda x: abs(x))

    if show_highly_correlated_varaibles:
        top_corr = {}
        for column in top_df.columns:
            if "abs_" not in column:
                temp_df = top_df[[column]]
                temp_df["abs_corr"] = top_df[column].apply(lambda x: abs(x))
                temp_df = temp_df[
                    (temp_df["abs_corr"] > 0.7) & (temp_df["abs_corr"] < 1)
                    ][column].reset_index().rename(columns={"index": "Correlated variable"})
                if temp_df.dropna().shape[0] > 0:
                    for idx, row in temp_df.iterrows():
                        if  (column + ", " + row["Correlated variable"] not in top_corr.keys()) and\
                            (row["Correlated variable"] + ", " + column not in top_corr.keys()):
                            top_corr[column + ", " + row["Correlated variable"]] = row[column]
        pd.set_option("display.max_rows", None)
        pd.set_option("display.max_colwidth", None)
        print("\n* All pairs of variables with absolute correlation coefficients > 0.7:\n")
        print(pd.DataFrame(data = top_corr.values(), index = top_corr.keys())\
            .rename(columns={0: "Correlation coefficient"})\
                .sort_values("Correlation coefficient", key=lambda x: abs(x), ascending=False))

    # Sort by absolute correlation coefficients and choose top n variables, excluding the variable itself.
    top_df = top_df.sort_values("abs_"+variable, ascending=False)[1:top_n+1].reset_index()
    top_df["Rank"] = top_df.index + 1

    # Print out the dataframe in a neat tabular format.
    print("\n" + f"\t<<Correlations with {variable}>>" + "\n")
    print(tabulate(top_df[["Rank", "index", variable]],
                    headers= ["Rank", "Correlated variable", "Correlation coefficient"],
                    showindex=False,
                    tablefmt='simple_grid',
                    floatfmt=',.7g'))


def create_interactive_taxi_socio(taxi_col, socio_col):
    """
    Interactively create a choropleth in HTML
    where one of the taxi trip variables and one of the socioeconomic variables are plotted on the same map
    to visualize how they are or are not related in each of the community districts and CDTAs.
    """

    taxi_cols = [
    'total_trip_count', 'PU_minute_per_mile', 'DO_minute_per_mile',
    'total_passenger_count', 'total_trip_distance (mile)',
    'total_fare', 'total_congestion_surcharge', 'total_airport_fee', 'total_duration (min)',
    'average_passenger_count', 'average_trip_distance (mile)', 'average_fare',
    'average_congestion_surcharge', 'average_airport_fee', 'average_duration (min)']

    socio_cols = [
            'Population',
            'Disabled population',
            'Foreign-born population',
            'Population aged 65+',
            'Median household income (2021$)',
            'Poverty rate',
            'Labor force participation rate',
            'Population aged 25+ without a high school diploma',
            'Unemployment rate',
            'Severely rent-burdened households',
            'Homeownership rate',
            'Severe crowding rate (% of renter households)',
            'Population density (1,000 persons per square mile)',
            'Car-free commute (% of commuters)',
            'Mean travel time to work (minutes)',
            'Serious crime rate (per 1,000 residents)']

    # Load and merge datasets.            
    cdta_df = load_cdta_df(folder_name="data\\cdta_df").set_geometry("geometry")
    socio_df = get_socio_df(cdta_df).set_geometry("geometry")
    nyc_df = gpd.read_file(gpd.datasets.get_path('nybb'))
    
    print("Combining the PU and DO columns, except minute per mile, by averaging the values in cdta_df.")
    for col in taxi_cols:
        if "minute_per_mile" not in col:
            try:
                cdta_df[col] = (cdta_df["PU_"+col] + cdta_df["DO_"+col]) / 2
                cdta_df.drop(["PU_"+col, "DO_"+col], axis=1, inplace=True)
            except KeyError:
                pass
            
    socio_df = pd.merge(nyc_df, socio_df, on="geometry", how="outer").iloc[5:, 4:]
    taxi_df = pd.merge(nyc_df, cdta_df, on="geometry", how="outer").iloc[5:, 4:]
    merged_df = pd.merge(
        taxi_df,
        socio_df.rename(columns={"Community District": "CDTA"}).drop(["borough", "geometry"], axis=1),
        on='CDTA',
        how='outer')[taxi_cols + socio_cols + ["CDTA", "CDTA_name", "borough", "geometry", "centroid"]]\
            .set_geometry("geometry")

    # Add rank columns based on taxi_col and socio_col.
    merged_df = merged_df.sort_values(taxi_col, ascending=False).reset_index(drop=True)
    merged_df["Rank: " + taxi_col] = [idx+1 for idx in merged_df.index]

    merged_df = merged_df.sort_values(socio_col, ascending=False).reset_index(drop=True)
    merged_df["Rank: " + socio_col] = [idx+1 for idx in merged_df.index]
    
    # merged_df[socio_cols] = merged_df[socio_cols].fillna("Not available")
    
    # Create a map for taxi_col.
    m = merged_df[["CDTA", "CDTA_name", "borough",
                    taxi_col, "Rank: " + taxi_col,
                    socio_col, "Rank: " + socio_col,
                    "geometry"]].explore(
        column=taxi_col,
        style_kwds=dict(color="black", weight=1), # use black outline
        legend_kwds=dict(colorbar=False, fmt="{:,}"),
        tooltip_kwds=dict(localize=True),
        tiles="CartoDB positron",
        scheme="naturalbreaks",  # use mapclassify's natural breaks scheme
        cmap='Reds',
        k=5, # bins
        name=taxi_col,
    )

    # Create a map for socio_col.
    merged_df["centroid"] = merged_df.geometry.centroid
    merged_df = merged_df.set_geometry("centroid")
    merged_df[["CDTA", "CDTA_name", "borough",
                taxi_col, "Rank: " + taxi_col,
                socio_col, "Rank: " + socio_col,
                "centroid"]].explore(
                column=socio_col,
                style_kwds=dict(color="black", weight=1), # use black outline
                legend_kwds=dict(colorbar=False, fmt="{:,}"),
                tooltip_kwds=dict(localize=True),
                tiles="CartoDB positron",
                scheme="naturalbreaks",  # use mapclassify's natural breaks scheme
                cmap='Blues',
                k=5, # bins
                name=socio_col,
                m=m,
                marker_type="circle",
                marker_kwds=dict(fill=True, radius=500)
            )
    
    folium.LayerControl().add_to(m)
    m.save('taxi_socio_map.html')
    webbrowser.open('taxi_socio_map.html')