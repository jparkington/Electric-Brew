1. Modify ERD, Data Dictionary to account for the new fields
2. Rework all sections of `/src/README.md` and add new regex
3. Add docstring and more detail to `etl.py`
4. Add quick links to the top-level README
5. Work out circular logic between `etl` and `variables` (since they both call `electric_brew.db`)