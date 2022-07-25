---
title: GeospatialServices - Flooding Risk
hide_title: true
status: stable
---
# Visualizing Customer addresses on a flood plane

King County (WA) publishes flood plain data as well as tax parcel data. We can use the addresses in the tax parcel data and use the geocoder to calculate coordinates. Using this coordinates and the flood plain data we can enrich out dataset with a flag indicating whether the house is in a flood zone or not.

The following data has been sourced from King County's Open data portal. [_Link_](https://data.kingcounty.gov/)
1. [Address Data](https://mmlspark.blob.core.windows.net/publicwasb/maps/KingCountyAddress.csv)
1. [Flood plains](https://mmlspark.blob.core.windows.net/publicwasb/maps/KingCountyFloodPlains.geojson)

For this demonstration, please follow the instructions on setting up your azure maps account from the overview notebook.

## Prerequisites
1. Upload the flood plains data as map data to your creator resource


```python
import os
import json
import time
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

# Configure more resiliant requests to stop flakiness
retry_strategy = Retry(
    total=3,
    status_forcelist=[429, 500, 502, 503, 504],
    method_whitelist=["HEAD", "GET", "PUT", "DELETE", "OPTIONS", "TRACE"],
)
adapter = HTTPAdapter(max_retries=retry_strategy)
http = requests.Session()
http.mount("https://", adapter)
http.mount("http://", adapter)

if os.environ.get("AZURE_SERVICE", None) == "Microsoft.ProjectArcadia":
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()
    from notebookutils.mssparkutils.credentials import getSecret

    os.environ["AZURE_MAPS_KEY"] = getSecret("mmlspark-build-keys", "azuremaps-api-key")
    from notebookutils.visualization import display


# Azure Maps account key
azureMapsKey = os.environ["AZURE_MAPS_KEY"]  # Replace this with your azure maps key

# Creator Geo prefix
# for this example, assuming that the creator resource is created in `EAST US 2`.
atlas_geo_prefix = "us"

# Load flood plains data
flood_plain_geojson = http.get(
    "https://mmlspark.blob.core.windows.net/publicwasb/maps/KingCountyFloodPlains.geojson"
).content

# Upload this flood plains data to your maps/creator account. This is a Long-Running async operation and takes approximately 15~30 seconds to complete
r = http.post(
    f"https://{atlas_geo_prefix}.atlas.microsoft.com/mapData/upload?api-version=1.0&dataFormat=geojson&subscription-key={azureMapsKey}",
    json=json.loads(flood_plain_geojson),
)

# Poll for resource upload completion
resource_location = r.headers.get("location")
for _ in range(20):
    resource = json.loads(
        http.get(f"{resource_location}&subscription-key={azureMapsKey}").content
    )
    status = resource["status"].lower()
    if status == "running":
        time.sleep(5)  # wait in a polling loop
    elif status == "succeeded":
        break
    else:
        raise ValueError("Unknown status {}".format(status))

# Once the above operation returns a HTTP 201, get the user_data_id of the flood plains data, you uploaded to your map account.
user_data_id_resource_url = resource["resourceLocation"]
user_data_id = json.loads(
    http.get(f"{user_data_id_resource_url}&subscription-key={azureMapsKey}").content
)["udid"]
```

Now that we have the flood plains data setup in our maps account, we can use the `CheckPointInPolygon` function to check if a location `(lat,lon)` coordinate is in a flood zone.

### Load address data:


```python
data = spark.read.option("header", "true").csv(
    "wasbs://publicwasb@mmlspark.blob.core.windows.net/maps/KingCountyAddress.csv"
)

# Visualize incoming schema
print("Schema:")
data.printSchema()

# Choose a subset of the data for this example
subset_data = data.limit(50)
display(subset_data)
```

### Wire-up the Address Geocoder

We will use the address geocoder to enrich the dataset with location coordinates of the addresses.


```python
from pyspark.sql.functions import col
from synapse.ml.cognitive import *
from synapse.ml.stages import FixedMiniBatchTransformer, FlattenBatch
from synapse.ml.geospatial import *


def extract_location_fields(df):
    # Use this function to select only lat/lon columns into the dataframe
    return df.select(
        col("*"),
        col("output.response.results")
        .getItem(0)
        .getField("position")
        .getField("lat")
        .alias("Latitude"),
        col("output.response.results")
        .getItem(0)
        .getField("position")
        .getField("lon")
        .alias("Longitude"),
    ).drop("output")


# Azure Maps geocoder to enhance the dataframe with location data
geocoder = (
    AddressGeocoder()
    .setSubscriptionKey(azureMapsKey)
    .setAddressCol("FullAddress")
    .setOutputCol("output")
)

# Set up a fixed mini batch transformer to geocode addresses
batched_dataframe = geocoder.transform(
    FixedMiniBatchTransformer().setBatchSize(10).transform(subset_data.coalesce(1))
)
geocoded_addresses = extract_location_fields(
    FlattenBatch().transform(batched_dataframe)
)

# Display the results
display(geocoded_addresses)
```

Now that we have geocoded the addresses, we can now use the `CheckPointInPolygon` function to check if a property is in a flood zone or not.

### Setup Check Point In Polygon 


```python
def extract_point_in_polygon_result_fields(df):
    # Use this function to select only lat/lon columns into the dataframe
    return df.select(
        col("*"),
        col("output.result.pointInPolygons").alias("In Polygon"),
        col("output.result.intersectingGeometries").alias("Intersecting Polygons"),
    ).drop("output")


check_point_in_polygon = (
    CheckPointInPolygon()
    .setSubscriptionKey(azureMapsKey)
    .setGeography(atlas_geo_prefix)
    .setUserDataIdentifier(user_data_id)
    .setLatitudeCol("Latitude")
    .setLongitudeCol("Longitude")
    .setOutputCol("output")
)


flood_plain_addresses = extract_point_in_polygon_result_fields(
    check_point_in_polygon.transform(geocoded_addresses)
)

# Display the results
display(flood_plain_addresses)
```

### Cleanup Uploaded User Data (Optional)
You can (optionally) delete the uploaded geojson polygon.


```python
res = http.delete(
    f"https://{atlas_geo_prefix}.atlas.microsoft.com/mapData/{user_data_id}?api-version=1.0&subscription-key={azureMapsKey}"
)
```
