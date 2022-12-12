import pandas as pd
import json
from mpl_toolkits.axes_grid1.axes_divider import make_axes_locatable
import matplotlib.pyplot as plt
import numpy as np
import plotly.graph_objs as go
import os
import plotly.express as px
import sys
import gdown
import seaborn as sns
import altair as alt
import geopandas as gpd
import json
import ipywidgets as widgets


colors = {'Bronx':'red', 'Manhattan':'green', 'Queens':'blue', 'Staten Island':'orange', 'Brooklyn':'purple'}


def fill_value(row):
    
    a = row['boro_cd'] 
    
    if row['boro_cd'] > 100:
        cd = 'BK'
        b = 100
    if row['boro_cd'] > 200:
        b = 200
        cd = 'BX'
    if row['boro_cd'] > 300:
        b = 300
        cd = 'MN'
    if row['boro_cd'] > 400:
        b = 400
        cd = 'QN'
    if row['boro_cd'] > 500:
        b = 500
        cd = 'SI'
    
    i = a % 100
    s_name = cd+  str(i).zfill(2)
    
    return s_name

