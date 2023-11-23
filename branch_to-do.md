1. Handle new split of `charge` and `kwh` for rate metrics in `dim_bills`
2. Incorporate a new `tax_cost` field and logic into `fct_electric_brew` (from `dim_bills`)
3. Modify ERD, Data Dictionary to account for the new fields
4. Test all curation scripts in `etl.py`
5. Add new regex commands to `/src/README.md`
6. Built out ETL and Variables section in that README
7. Rework `curated` docs and docstring to reflect the two helper functions
8. Add quick links to the top-level README