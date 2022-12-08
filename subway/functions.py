import rtree
import pygeos
import pandas as pd
from fuzzywuzzy import fuzz
from fuzzywuzzy import process
import geopandas as gpd
import matplotlib.pyplot as plt
import numpy as nump
import pathlib


from mpl_toolkits.axes_grid1 import make_axes_locatable
from shapely.geometry import Point, Polygon

def removeSpecialCharactersFromStationName(df):
    """
    Remove special characters from station names.
    """
    df.station = df.station.str.replace("/","_")
    df.station = df.station.str.replace("-","_")
    df.station = df.station.str.replace(" ","_")
    df.station = df.station.str.lower()
    return df


def removeSpecialCharactersFromStationTimestamps(df):
    """
    Remove special characters from station timestamps.
    """
    df['date'] = df['date'].str.replace('/','_')
    df['time'] = df['time'].str.replace(':','_')
    df['desc'] = df['desc'].str.replace(' ', '_')
    return df


def createStationCountsByTime(df, input_col, col_name='entry'):
    """
    Create station timestamp column into multiple Date columns like Hour, weekday, day of the week.
    """
    df[col_name+'_day'] = df[input_col].dt.day
    df[col_name+'_hour'] = df[input_col].dt.hour
    df[col_name+'_weekday'] = df[input_col].dt.day_name()
    df[col_name+'_year_month'] = df[input_col].dt.to_period('M')
    return df

def computeTrafficRidershipCounts(df):
    """
    Compute Net entries, Net Exits, Net Traffic columns.
    """
    df['net_entries'] = df.groupby(['control_area', 'unit', 'subunit_channel_pos', 'station'])['entries'].transform(lambda x: x.diff())
    df['net_exits'] = df.groupby(['control_area', 'unit', 'subunit_channel_pos', 'station'])['exits'].transform(lambda x: x.diff())
    df['net_traffic'] = df.net_entries + df.net_exits

    # Elimating turnstiles that count in reverse by casting all values as absolutes.
    df['net_entries'] = abs(df.net_entries)
    df['net_exits'] = abs(df.net_exits)
    df['net_traffic'] = abs(df.net_traffic)
    return df

def removeOutlier(df):

    """
    Remove outlier data from ridership counts.
    """
    q = nump.nanquantile(df["net_entries"], .99)
    df = df[df["net_entries"] < q]

    q2 = nump.nanquantile(df["net_exits"], .99)
    df = df[df["net_exits"] < q2]

    q3 = nump.nanquantile(df['net_traffic'], .99)
    df=df[df['net_traffic'] < q3]
    return df

def removeSpecialCharactersFromStopName(df):
    """
    Remove special characters from stop names.
    """   
    df.stop_name = df.stop_name.str.replace(" - ","_")
    df.stop_name = df.stop_name.str.replace(" ","_")
    df.stop_name = df.stop_name.str.replace("(","")
    df.stop_name = df.stop_name.str.replace(")","")
    df.stop_name = df.stop_name.str.replace("/","_")
    df.stop_name = df.stop_name.str.replace(".","")
    df.stop_name = df.stop_name.str.replace("-","_")
    df.stop_name = df.stop_name.str.lower()
    return df



def matchStationNames(df,df_gtfs):
    """
    Match station names and GTFS stopnames which has latitude and longitude

    """
    mat1 = []
    mat2 = []
    p= []
    list1 = df.station.tolist()
    list2 = df_gtfs.stop_name.tolist()
 
    threshold = 50

    for i in list1:
        mat1.append(process.extractOne(i, list2, scorer=fuzz.ratio))
    df['matches'] = mat1

    for j in df['matches']:
        if j[1] >= threshold:
            p.append(j[0])

        mat2.append(','.join(p))
        p= []

    df['matches'] = mat2
    return df,df_gtfs



def combineGTFSStopsAndStationData(df):
    """
    Combine Station and Stop data
    
    """  
   
    df['geometry'] = [Point(xy) for xy in zip(nump.array(df['gtfs_longitude']), nump.array(df['gtfs_latitude']))]
    gpd.options.use_pygeos = True
    
    cdta_file_path = str(pathlib.Path("data/nycdta2020_22b", "nycdta2020.shp"))
    cdta_map = gpd.read_file(cdta_file_path)
    cdta_map.to_crs(4326, inplace=True)
    
    cdta_geo_df = cdta_map[['CDTA2020', 'CDTAName','geometry', 'Shape_Leng', 'Shape_Area','BoroName']].set_index('CDTA2020', drop=True)
    
    top_station_geo_df = gpd.GeoDataFrame(df, crs=4326, geometry = df.geometry)
    top_station_geo_df.to_crs(4326, inplace=True)
    
    # Locate each Station Point Geometry within NTA Polygon geometry
    station_all_df = gpd.sjoin(cdta_geo_df,top_station_geo_df, how="left", op="contains")
    station_all_df = station_all_df.reset_index()
    
    station_all_df = station_all_df[station_all_df['CDTA2020'].str.match('^[a-zA-Z]{2}\d{2}$')]
  
    #Few stations that belong to Manhattan Burough were identified based on the CDTA code
    station_all_df['borough'] = station_all_df.borough.fillna("M")

    cdta_dict = cdta_map[["CDTA2020", "CDTAName"]].set_index("CDTA2020").to_dict()["CDTAName"]
    return station_all_df,cdta_dict  

