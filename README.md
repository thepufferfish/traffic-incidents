# Traffic fatalities project

This repo holds code for a project that aims to use traffic incident data, streets networks data, and satellite imagery to predict dangerous intersections as measured by traffic fatalities. The data is currently limited to Sacramento, CA but could be expanded to all of California. Right now, it runs in a Docker container on a HP mini PC set up with Ubuntu. The data pipeline is set up using [Dagster](https://dagster.io/).

## Getting started

First, clone the repo, then navigate to the project directory. To install dependencies, run:

```bash
pip install -e ".[dev]"
```

Then, start the Dagster UI web server:

```bash
dagster dev
```

Open http://localhost:3000 with your browser to see the project.

## Data sources

* Traffic incident data is downloaded via [UC Berkeley's Transportation Injury Mapping System](https://tims.berkeley.edu/) or TIMS which geocodes data from  California Statewide Integrated Traffic Records System (SWITRS).
* Street network is accessed from [Open Street Map](https://www.openstreetmap.org/) or OSM using the Python package [osmnx](https://osmnx.readthedocs.io/en/stable/) which accesses the OSM API, converts the data to a network graph, and provides tools for working with its street network data.
* Satellite images of intersections are downloaded via [Mapbox](https://www.mapbox.com/) API.

## TODO:

* Analyze features of OSM data
* Build baseline predictive model
* Explore Graph Neural Networks (GNNs) 
* Test augmenting GNNs with Convolutional Neural Networks (CNNs) using satellite imagery to improve predictive power
