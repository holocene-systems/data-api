# 3RWW API

The 3RWW Data API provides low- and high-level views of environmental and infrastructure data for the Greater Pittsburgh Area, including:

* realtime and historic rainfall
* storm and wastewater systems
* terrain and landcover

This is currently a work-in-progress.

## Existing endpoints

### Rainfall: `/rainfall`

3 Rivers Wet Weather uses calibrated data from the NEXRAD radar located in Moon Township, PA with rain gauge measurements collected during the same time period and rain event for every square kilometer in Allegheny County. The resulting rainfall data is equivalent in accuracy to having 2,276 rain gauges placed across the County. 3RWW has a massive repository of this high resolution spatiotemporal calibrated radar rainfall data for Allegheny County dating back to 2000 and including nearly 2 billion data points.

Rainfall data is provided by ALCOSAN, Vieux Associates, and Datawise. The real-time data provides real-time rainfall estimates; the historic data is based on the real-time data, with extensive QA/QC and calibration performed by experts at ALCOSAN and Vieux Associates.

The 3RWW Rainfall API provides access to both the real-time (raw) and historic (calibrated) rainfall data for the physical rain gauges and virtual, radar-based rainfall estimates data (a.k.a., gauge-adjusted radar rainfall data), as well as discrete rainfall events as identified by Vieux Associates.

## Planned Endpoints:

* Rainways: `/rainways` - composite data resources and analysis for the 3RWW Rainways toolset.
* Sewer Atlas: `/seweratlas` - 3RWW network trace analysis from the 3RWW Sewer Atlas.