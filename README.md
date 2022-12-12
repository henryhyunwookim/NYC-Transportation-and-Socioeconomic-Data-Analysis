# Analysis of inequalities in current access to transportation infrastructure in the city of New York

MADS_SIADS699_Fall2022_I:heart:NY<br>
Malarvizhi Veerappan, Hyunwoo Kim, Rajesh Panchu<br>
<br>
<b>Description</b><br>
This project aims to analyze access to transport infrastructure in the city of New York and assess whether the transport systems are accessible to all types of populations, especially those from lower socioeconomic backgrounds and other vulnerable populations.<br>
<br>
<b>Data Sources and Analyses</b><br>
The project primarily consists of three sets of analyses as follows:<br>
<ul>
1. Analyses on taxi trip data, sourced from the <a href="https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page">NYC Taxi and Limousine Commission (TLC)</a><br>
2. Analyses on subway ride data, sourced from the <a href="https://transitfeeds.com/p/mta">NYC Metropolitan Transportation Authority (MTA)</a><br>
3. Analyses on socioeconomic data, sourced from the <a href="https://furmancenter.org/">NYU Furman Center</a><br>
</ul>
Each set of analyses and how to load, clean, and process datasets can be found in the Jupyter Notebooks (i.e. .ipynb files) in the respective folder (i.e. "taxi", "subway", and "socio").<br>
Raw and processed data files are stored in the "data" folder under the respective folder so that the processes and analyses in the notebooks can be replicated.<br>
<br>
<b>Installation</b><br>
To run the notebooks, please clone or fork this repo and install the required packages by running the following command:<br>
pip install -r requirements.txt<br>
<br>
<b>Interactive Notebooks</b><br>
The notebooks with interactive visualizations can be run on a virtual environment provided by Google Colab.<br>
<br>
Here are the available notebooks:<br>
<ul>
1. <a href="https://colab.research.google.com/github/henryhyunwookim/NYC-Transportation-and-Socioeconomic-Data-Analysis/blob/main/taxi/interactive_taxi_colab.ipynb">Taxi analysis</a><br>
2. <a href="https://colab.research.google.com/github/henryhyunwookim/NYC-Transportation-and-Socioeconomic-Data-Analysis/blob/main/subway/interactive_subway_colab.ipynb">Subway analysis</a><br>
3. <a href="https://colab.research.google.com/github/henryhyunwookim/NYC-Transportation-and-Socioeconomic-Data-Analysis/blob/main/socio/interactive_socio_colab.ipynb">Socio-economic analysis with taxi and subway data</a><br>
</ul>
Here is the instruction of how to run the interactive notebooks:<br>
<ul>
1. Choose the topic of your interest from the list above and click the link to open the Google Golab notebook.<br>
2. Run the first code cell in the notebook to install requirements. It will clone our GitHub repository in the virtual environment, so please only run it once during the same session/runtime; otherwise it will keep creating a clone.<br>
3. Restart runtime when you see the following message after installation is completed:<br>
&emsp;<i>You must restart the runtime in order to use newly installed versions.</i><br>
4. Now you can run any code cell except the first one and explore our interactive visualizations!<br>
</ul>