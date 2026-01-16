# Overview

The psrc/hb-1491 repository is designed to evaluate and process transportation stops, land use, and development potential. It incorporates analyses such as determining buffer sizes for transit stops, evaluating the floor-area ratio (FAR) for parcels, and integrating demographic/land-use data with transit services.

Core functionalities include:
1. **Transit Stops Analysis:**
   - Categorizes transit stops and creates buffer zones based on specifications such as population thresholds, urban zoning, and transit mode.
   - Data sources include GTFS files for different years.

2. **Land Use and FAR Evaluation:**
   - Reads, merges, and processes geographic data to calculate maximum FAR for residential and mixed-use parcels. Assesses compliance with House Bill 1491 guidelines.
   - Includes detailed evaluations such as parcel size thresholds (e.g., 10k sqft).

3. **Geo-spatial Outputs:**
   - Generates outputs in geospatial formats such as OpenFileGDB for integration into GIS tools.
