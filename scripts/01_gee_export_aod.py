# -*- coding: utf-8 -*-
"""
This script exports annual mean Aerosol Optical Depth (AOD) data from 
Google Earth Engine (GEE) for the specified years and region.

It applies a quality assurance (QA) mask to filter for clear, 
high-quality, land-based pixels before calculating the annual mean.
The resulting GeoTIFF files are exported to the user's Google Drive.
"""
import ee
import time

print("--- Script Start: Exporting yearly AOD data with verified logic ---")

# 1. Authenticate and Initialize GEE
try:
    ee.Initialize()
except Exception as e:
    ee.Authenticate()
    ee.Initialize()

# 2. Define study area and parameters
usa = ee.FeatureCollection("USDOS/LSIB_SIMPLE/2017").filter(ee.Filter.eq('country_na', 'United States')).geometry()
collection_id = 'MODIS/061/MCD19A2_GRANULES'
aod_band = 'Optical_Depth_055'
scale_factor = 0.001
export_scale = 1000  # Export resolution in meters

# 3. Quality assurance mask function
def maskAOD(image):
  """Applies the QA mask based on the GEE Catalog documentation for MCD19A2.061."""
  qa = image.select('AOD_QA')
  
  # Condition 1: Pixel must be clear (Cloud Mask, Bits 0-2, value 1)
  cloudMask = qa.bitwiseAnd(7).eq(1)
  
  # Condition 2: Pixel must be land (Land/Water Mask, Bits 3-4, value 0)
  landMask = qa.rightShift(3).bitwiseAnd(3).eq(0)
  
  # Condition 3: AOD retrieval must be of best quality (AOD QA, Bits 8-11, value 0)
  qualityMask = qa.rightShift(8).bitwiseAnd(15).eq(0)

  # Combine all masks
  finalMask = cloudMask.And(landMask).And(qualityMask)
  
  return image.updateMask(finalMask)

# 4. Loop through each year and submit export tasks
start_year = 2015
end_year = 2024

for year in range(start_year, end_year + 1):
    start_time = time.time()
    print(f"Submitting export task for year: {year}...")

    # Define the date range for the current year
    start_date = f'{year}-01-01'
    end_date = f'{year}-12-31'

    # Create the annual mean composite image with the QA mask applied
    annual_mean_aod = (ee.ImageCollection(collection_id)
                       .filterDate(start_date, end_date)
                       .filterBounds(usa)
                       .map(maskAOD) # Apply the QA mask to each image
                       .select(aod_band)
                       .mean() # Calculate the annual mean
                       .multiply(scale_factor)) # Apply the scale factor

    # Define the export task
    task = ee.batch.Export.image.toDrive(
        image=annual_mean_aod.clip(usa),
        description=f'Annual_Mean_AOD_{year}_USA',
        folder='GEE_Exports', # Folder name in your Google Drive
        fileNamePrefix=f'AOD_{year}_USA',
        scale=export_scale,
        crs='EPSG:4326',
        maxPixels=1e13
    )

    # Start the task
    task.start()
    
    end_time = time.time()
    print(f"  - Task for year {year} submitted in {end_time - start_time:.2f} seconds.")

print("\n--- All export tasks have been submitted. ---")
print("Please go to the Google Earth Engine Code Editor website (code.earthengine.google.com)")
print("and click 'RUN' on the tasks in the 'Tasks' tab on the right-hand side.")