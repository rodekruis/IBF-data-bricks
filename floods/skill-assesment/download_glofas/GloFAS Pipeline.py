# Databricks notebook source
import cdsapi
import xarray as xr
import os
import zipfile
import shutil
from datetime import date
import time

# COMMAND ----------

# MAGIC %fs ls dbfs:/mnt/GloFAS/glofas/

# COMMAND ----------

df = spark.read.option("header",True).csv('/mnt/GloFAS/glofas/cdsapirc.csv')
Key = df.select('key').first()[0]
Url = df.select('url').first()[0]

# COMMAND ----------

output_path = "/dbfs/mnt/GloFAS/glofas/"
CURRENT_DATE = date.today()
current_date = CURRENT_DATE.strftime('%Y%m%d')

def start_download_loop():
  downloadDone = False

  timeToTryDownload =  43200
  timeToRetry = 600

  start = time.time()
  end = start + timeToTryDownload

  while downloadDone == False and time.time() < end:
      try:
          make_api_request()
          downloadDone = True
      except:
          error = 'Download data failed. Trying again in 10 minutes.'
          print(error)
          time.sleep(timeToRetry)
  if downloadDone == False:
      raise ValueError('GLofas download failed for ' +
                      str(timeToTryDownload/3600) + ' hours, no new dataset was found')

def make_api_request():
  r = c.retrieve(
            'cems-glofas-forecast',
            {
                'system_version': 'version_2_1',
                'variable': 'river_discharge_in_the_last_24_hours',
                'format': 'netcdf',
                'product_type': 'ensemble_perturbed_forecasts',
                'year': str("{:04d}".format(CURRENT_DATE.year)),
                'month': str("{:02d}".format(CURRENT_DATE.month)),
                'day': str("{:02d}".format(CURRENT_DATE.day)),
                'leadtime_hour': [
                    '24', '48', '72',
                    '96', '120', '144',
                    '168',
                ],
                'hydrological_model': 'htessel_lisflood',
                'area': [15, 29, -4, 48]
            },
            ("%sglofas-forecast-%s.nc" % (output_path, current_date)))
    
c = cdsapi.Client(key=Key, url=Url)

if True:
    start_download_loop()

# COMMAND ----------


