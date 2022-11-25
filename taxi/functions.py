import requests
import bs4
from collections import defaultdict
from tqdm import tqdm
import os
import dask.dataframe as dd
import geopandas as gpd
import pandas as pd
from zipfile import ZipFile
import matplotlib
import matplotlib.pyplot as plt
import imageio.v2 as imageio
from mpl_toolkits.axes_grid1 import make_axes_locatable


def load_taxi_trip_data(source_url, folder_name="Download"):
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
            path = os.getcwd() + "\\" + folder_name + "\\" + taxi_type
            if not os.path.exists(path):
                os.makedirs(path)
                print(f"Folder created: {path}")

            file_path = path + "\\" + filename
            if not os.path.isfile(file_path):
                res = requests.get(url, allow_redirects=True)
                with open(file_path, 'wb') as file:
                    file.write(res.content)
                print(f"{filename} downloaded in {path}.")
            else:
                print(f"{filename} already exists in {path}.")

            df = dd.read_parquet(path + "\\" + filename, ignore_metadata_file=True, split_row_groups=True)
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


def load_taxi_zones_shp(source_url, folder_name="Download", target_filename = "taxi_zones.zip"):
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
            path = os.getcwd() + "\\" + folder_name
            if not os.path.exists(path):
                os.makedirs(path)
                print(f"Folder created: {path}")

            file_path = path + "\\" + target_filename
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


def get_cdta_df(street_hail_df, cdta_geo_dict, taxi_zones_df, folder_name="Download"):
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
        path = os.getcwd() + "\\" + folder_name
        if not os.path.exists(path):
            os.makedirs(path)
            print(f"Folder created: {path}")
        cdta_df.to_csv(f'{path}/cdta_df.csv')

        print(f"cdta_df saved in {path}.")
    
    return cdta_df


def get_cdta_df_per_month(street_hail_df, cdta_geo_dict, taxi_zones_df, folder_name="Download"):
    path = os.getcwd() + "\\" + folder_name
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


def load_cdta_df(folder_name="Download"):
    path = os.getcwd() + "\\" + folder_name
    cdta_df = pd.read_csv(f'{path}/cdta_df.csv', index_col=0)

    # Make the loaded csv file into a geo dataframe.
    cdta_df['geometry'] = gpd.GeoSeries.from_wkt(cdta_df['geometry'])
    cdta_df = gpd.GeoDataFrame(cdta_df, geometry='geometry')
    
    print(f"cdta_df loaded from {path}.")

    return cdta_df


def load_cdta_df_per_month(folder_name="Download",\
    year_month_list = ['2022-01', '2022-02', '2022-03', '2022-04', '2022-05', '2022-06']):
    path = os.getcwd() + "\\" + folder_name

    dfs = []
    year_months = []
    for location_type in ["PU", "DO"]:
        for year_month in year_month_list:
            file_path = f'{path}/cdta_df_{location_type}_{year_month}.csv'
            dfs.append(pd.read_csv(file_path, index_col=0))
            year_months.append(f"{location_type}_{year_month}")

            print(f"cdta_df_{location_type}_{year_month}.csv loaded from {path}.")
        
    return dfs, year_months


def plot_total_trips(cdta_df, pu_do, single_month, year_month, save_png):
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
    
    if single_month:
        fig, axs = plt.subplots(3, 2, figsize=(20, 20))
        total_day_df.plot(ax=axs[0, 0])
        axs[0, 0].set_title(f"total_trip_count_day_{year_month}")

        total_hour_df.plot(ax=axs[1, 0])
        axs[1, 0].set_title(f"total_trip_count_hour_{year_month}")

        total_weekday_df.plot(ax=axs[2, 0])
        axs[2, 0].set_title(f"total_trip_count_weekday_{year_month}")
        
        (total_day_df / 6).plot(ax=axs[0, 1])
        axs[0, 1].set_title(f"monthly_average_count_day_{year_month}")

        (total_hour_df / 6).plot(ax=axs[1, 1])
        axs[1, 1].set_title(f"monthly_average_count_hour_{year_month}")

        (total_weekday_df / 6).plot(ax=axs[2, 1])
        axs[2, 1].set_title(f"monthly_average_count_weekday_{year_month}")
        
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
        axs[0, 0].set_title(f"{pu_do}_total_trip_count_day")

        total_hour_df.plot(ax=axs[1, 0])
        axs[1, 0].set_title(f"{pu_do}_total_trip_count_hour")

        total_weekday_df.plot(ax=axs[2, 0])
        axs[2, 0].set_title(f"{pu_do}_total_trip_count_weekday")

        total_year_month_df.plot(ax=axs[3, 0])
        axs[3, 0].set_title(f"{pu_do}_total_trip_count_year_month")
        
        (total_day_df / 6).plot(ax=axs[0, 1])
        axs[0, 1].set_title(f"{pu_do}_monthly_average_count_day")

        (total_hour_df / 6).plot(ax=axs[1, 1])
        axs[1, 1].set_title(f"{pu_do}_monthly_average_count_hour")

        (total_weekday_df / 6).plot(ax=axs[2, 1])
        axs[2, 1].set_title(f"{pu_do}_monthly_average_count_weekday")
        
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
                label,                      # Use `label` as label
                (x_value, y_value),         # Place label at end of the bar
                xytext=(0, space),          # Vertically shift label by `space`
                textcoords="offset points", # Interpret `xytext` as offset in points
                ha='center',                # Horizontally center label
                va=va)               
        ax.set_title(f"{pu_do}_monthly_average_trip_count_per_borough");       
        
    if year_month == None:
        year_month = "Jan-Jun 2022"
    if save_png:
        matplotlib.use('Agg')
        path = os.getcwd() + "\\" + "png"
        if not os.path.exists(path):
            os.makedirs(path)
        filepath = f'{path}/{year_month}.png'
        chart = fig.get_figure()
        chart.savefig(filepath, dpi=300)
        print(f"{year_month}.png saved in {path}.")
        plt.close(fig)


def plot_trips_per_month(dfs, year_months, pu_do):
    if pu_do == "PU":
        dfs = dfs[:int(len(dfs)/2)]
        year_months = year_months[:int(len(year_months)/2)]
    elif pu_do == "DO":
        dfs = dfs[int(len(dfs)/2):]
        year_months = year_months[int(len(year_months)/2):]
        
    for df, year_month in zip(dfs, year_months):
        plot_total_trips(df, pu_do=pu_do, single_month=True, year_month=year_month, save_png=True)
        
    # Create GIF using PNG files
    print("Getting a GIF file using the PNG files.")
    images = []
    path = os.getcwd() + "\\" + "png"
    for file_name in sorted(os.listdir(path)):
        if (file_name.endswith(".png")) and (pu_do in file_name):
            file_path = os.path.join(path, file_name)
            images.append(imageio.imread(file_path))
    imageio.mimsave(path+'\\trip_counts_per_month.gif', images, duration=2)


def plot_on_map(df, pu_do):
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

    fig, axes = plt.subplots(7, 2, figsize=(15,50))
    for i, col in enumerate(cols):
        if i < 7:
            ax = axes[i%7, 0]
        else:
            ax = axes[i%7, 1]
        divider = make_axes_locatable(ax)
        cax = divider.append_axes("bottom", size="5%", pad=0.5)
        vmin, vmax = df[col].min(), df[col].max()
        df[[col, 'geometry']].plot(
            column=col,
            ax=ax,
            cax=cax,
            legend=True,
            legend_kwds={'label': col,
                        'orientation': 'horizontal'},
            vmin=vmin,
            vmax=vmax
        )