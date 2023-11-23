1. Modify ERD, Data Dictionary to account for the new fields
2. Add new regex commands to `/src/README.md`
3. Build out ETL and Variables section in that README
4. Rework `curated` docs and docstring to reflect the two helper functions
5. Add docstring and more detail to `etl.py`
6. Add quick links to the top-level README
7. Work out circular logic between `etl` and `variables` (since they both call `electric_brew.db`)