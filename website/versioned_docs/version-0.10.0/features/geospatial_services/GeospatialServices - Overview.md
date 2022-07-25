---
title: GeospatialServices - Overview
hide_title: true
status: stable
---
<img width="500"  src="https://azurecomcdn.azureedge.net/cvt-18f087887a905ed3ae5310bee894aa53fc03cfffadc5dc9902bfe3469d832fec/less/images/section/azure-maps.png" />

# Azure Maps Geospatial Services

[Microsoft Azure Maps ](https://azure.microsoft.com/en-us/services/azure-maps/) provides developers from all industries with powerful geospatial capabilities. Those geospatial capabilities are packed with the freshest mapping data. Azure Maps is available for web, mobile (iOS and Android), Microsoft Power BI, Microsoft Power Apps and Microsoft Synapse. Azure Maps is an Open API compliant set of REST APIs. The following are only a high-level overview of the services which Azure Maps offers - Maps, Search, Routing, Traffic, Weather, Time Zones, Geolocation, Geofencing, Map Data, Creator, and Spatial Operations.

## Usage

### Geocode addresses
[**Address Geocoding**](https://docs.microsoft.com/en-us/rest/api/maps/search/post-search-address-batch) The Search Address Batch API sends batches of queries to Search Address API using just a single API call. This API geocodes text addresses or partial addresses and the geocoding search index will be queried for everything above the street level data. **Note** that the geocoder is very tolerant of typos and incomplete addresses. It will also handle everything from exact street addresses or street or intersections as well as higher level geographies such as city centers, counties, states etc.

### Reverse Geocode Coordinates
[**Reverse Geocoding**](https://docs.microsoft.com/en-us/rest/api/maps/search/post-search-address-reverse-batch) The Search Address Reverse Batch API sends batches of queries to Search Address Reverse API using just a single API call. This API takes in location coordinates and translates them into human readable street addresses. Most often this is needed in tracking applications where you receive a GPS feed from the device or asset and wish to know what address where the coordinate is located.

### Get Point In Polygon
[**Get Point in Polygon**](https://docs.microsoft.com/en-us/rest/api/maps/spatial/get-point-in-polygon) This API returns a boolean value indicating whether a point is inside a set of polygons. The set of polygons can we pre-created by using the [**Data Upload API**](https://docs.microsoft.com/en-us/rest/api/maps/data/upload-preview)  referenced by a unique udid.

## Prerequisites

1. Sign into the [Azure Portal](https://portal.azure.com) and create an Azure Maps account by following these [instructions](https://docs.microsoft.com/en-us/azure/azure-maps/how-to-manage-account-keys#create-a-new-account).
1. Once the Maps account is created, provision a Maps Creator Resource by following these [instructions](https://docs.microsoft.com/en-us/azure/azure-maps/how-to-manage-creator#create-creator-resource). Creator is a [geographically scoped service](https://docs.microsoft.com/en-us/azure/azure-maps/creator-geographic-scope). Pick appropriate location while provisioning the creator resource. 
1. Follow these [instructions](https://docs.microsoft.com/en-us/azure/cognitive-services/big-data/getting-started#create-an-apache-spark-cluster) to set up your Azure Databricks environment and install SynapseML.
1. After you create a new notebook in Azure Databricks, copy the **Shared code** below and paste into a new cell in your notebook.
1. Choose a service sample, below, and copy paste it into a second new cell in your notebook.
1. Replace the `AZUREMAPS_API_KEY` placeholders with your own [Maps account key](https://docs.microsoft.com/en-us/azure/azure-maps/how-to-manage-authentication#view-authentication-details).
1. Choose the run button (triangle icon) in the upper right corner of the cell, then select **Run Cell**.
1. View results in a table below the cell.

## Shared code

To get started, we'll need to add this code to the project:


```python
from pyspark.sql.functions import udf, col
from pyspark.sql.types import StructType, StructField, DoubleType
from pyspark.sql.functions import lit
from pyspark.ml import PipelineModel
from pyspark.sql.functions import col
import os
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
```


```python
if os.environ.get("AZURE_SERVICE", None) == "Microsoft.ProjectArcadia":
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()
    from notebookutils.mssparkutils.credentials import getSecret

    os.environ["AZURE_MAPS_KEY"] = getSecret("mmlspark-build-keys", "azuremaps-api-key")
    from notebookutils.visualization import display
```


```python
from synapse.ml.cognitive import *
from synapse.ml.geospatial import *

# An Azure Maps account key
azureMapsKey = os.environ["AZURE_MAPS_KEY"]
```

## Geocoding sample

The azure maps geocoder sends batches of queries to the [Search Address API](https://docs.microsoft.com/en-us/rest/api/maps/search/getsearchaddress). The API limits the batch size to 10000 queries per request.  


```python
from synapse.ml.stages import FixedMiniBatchTransformer, FlattenBatch

df = spark.createDataFrame(
    [
        ("One, Microsoft Way, Redmond",),
        ("400 Broad St, Seattle",),
        ("350 5th Ave, New York",),
        ("Pike Pl, Seattle",),
        ("Champ de Mars, 5 Avenue Anatole France, 75007 Paris",),
    ],
    [
        "address",
    ],
)


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


# Run the Azure Maps geocoder to enhance the data with location data
geocoder = (
    AddressGeocoder()
    .setSubscriptionKey(azureMapsKey)
    .setAddressCol("address")
    .setOutputCol("output")
)

# Show the results of your text query in a table format
display(
    extract_location_fields(
        geocoder.transform(FixedMiniBatchTransformer().setBatchSize(10).transform(df))
    )
)
```

## Reverse Geocoding sample

The azure maps reverse geocoder sends batches of queries to the [Search Address Reverse API](https://docs.microsoft.com/en-us/rest/api/maps/search/get-search-address-reverse) using just a single API call. The API allows caller to batch up to 10,000 queries per request


```python
# Create a dataframe that's tied to it's column names
df = spark.createDataFrame(
    (
        (
            (48.858561, 2.294911),
            (47.639765, -122.127896),
            (47.621028, -122.348170),
            (47.734012, -122.102737),
        )
    ),
    StructType([StructField("lat", DoubleType()), StructField("lon", DoubleType())]),
)

# Run the Azure Maps geocoder to enhance the data with location data
rev_geocoder = (
    ReverseAddressGeocoder()
    .setSubscriptionKey(azureMapsKey)
    .setLatitudeCol("lat")
    .setLongitudeCol("lon")
    .setOutputCol("output")
)

# Show the results of your text query in a table format

display(
    rev_geocoder.transform(FixedMiniBatchTransformer().setBatchSize(10).transform(df))
    .select(
        col("*"),
        col("output.response.addresses")
        .getItem(0)
        .getField("address")
        .getField("freeformAddress")
        .alias("In Polygon"),
        col("output.response.addresses")
        .getItem(0)
        .getField("address")
        .getField("country")
        .alias("Intersecting Polygons"),
    )
    .drop("output")
)
```

## Check Point In Polygon sample

This API returns a boolean value indicating whether a point is inside a set of polygons. The polygon can be added to you creator account using the [**Data Upload API**](https://docs.microsoft.com/en-us/rest/api/maps/data/upload-preview). The API then returnrs a unique udid to reference the polygon.

### Setup geojson Polygons in your azure maps creator account

Based on where the creator resource was provisioned, we need to prefix the appropriate geography code to the azure maps URL. In this example, the assumption is that the creator resource was provisioned in `East US 2` Location and hence we pick `us` as our geo prefix. 


```python
import time
import json

# Choose a geography, you want your data to reside in.
# Allowed values
# us => North American datacenters
# eu -> European datacenters
url_geo_prefix = "us"

# Upload a geojson with polygons in them
r = http.post(
    f"https://{url_geo_prefix}.atlas.microsoft.com/mapData/upload?api-version=1.0&dataFormat=geojson&subscription-key={azureMapsKey}",
    json={
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "properties": {"geometryId": "test_geometry"},
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [
                        [
                            [-122.14290618896484, 47.67856488312544],
                            [-122.03956604003906, 47.67856488312544],
                            [-122.03956604003906, 47.7483271435476],
                            [-122.14290618896484, 47.7483271435476],
                            [-122.14290618896484, 47.67856488312544],
                        ]
                    ],
                },
            }
        ],
    },
)

long_running_operation = r.headers.get("location")
time.sleep(30)  # Sometimes this may take upto 30 seconds
print(f"Status Code: {r.status_code}, Long Running Operation: {long_running_operation}")
# This Operation completes in approximately 5 ~ 15 seconds
user_data_id_resource_url = json.loads(
    http.get(f"{long_running_operation}&subscription-key={azureMapsKey}").content
)["resourceLocation"]
user_data_id = json.loads(
    http.get(f"{user_data_id_resource_url}&subscription-key={azureMapsKey}").content
)["udid"]
```

### Use the function to check if point is in polygon


```python
# Create a dataframe that's tied to it's column names
df = spark.createDataFrame(
    (
        (
            (48.858561, 2.294911),
            (47.639765, -122.127896),
            (47.621028, -122.348170),
            (47.734012, -122.102737),
        )
    ),
    StructType([StructField("lat", DoubleType()), StructField("lon", DoubleType())]),
)

# Run the Azure Maps geocoder to enhance the data with location data
check_point_in_polygon = (
    CheckPointInPolygon()
    .setSubscriptionKey(azureMapsKey)
    .setGeography(url_geo_prefix)
    .setUserDataIdentifier(user_data_id)
    .setLatitudeCol("lat")
    .setLongitudeCol("lon")
    .setOutputCol("output")
)

# Show the results of your text query in a table format
display(
    check_point_in_polygon.transform(df)
    .select(
        col("*"),
        col("output.result.pointInPolygons").alias("In Polygon"),
        col("output.result.intersectingGeometries").alias("Intersecting Polygons"),
    )
    .drop("output")
)
```

### Cleanup


```python
res = http.delete(
    f"https://{url_geo_prefix}.atlas.microsoft.com/mapData/{user_data_id}?api-version=1.0&subscription-key={azureMapsKey}"
)
```
