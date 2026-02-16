## analyses 📁
- A place for SQL files that you don't want to expose
- Generally use it for data quality reports
- Lots of people don't use it

- sql scripts
- not necessarily want to share, but usefufl to have around
- potentially do data quality check, sort of administrative tasks, something not purely test, something that want to share with end stakeholders

## dbt_project.yml
- The most important file in dbt
    - name of project, name of profile, set defaults, set variables
- No dbt_project.yml, tasks fail
- Tell dbt some defaults
- Need it to run dbt commands
- For dbt core, your profile should match the one in the `.dbt/profiles.yml`

## macros 📁
- They behave like Python functions (reusable logic)
- They help you encapsualte logic (in one place)
- They can be tested 

## README.md
- The documentation of your project
- Installation/setup guides
- Contact information
    - Information of operation: onboarding, credentials...

## seeds 📁
- A space to upload csv and flat files (to add them to dbt later)
- Quick and dirty approach (better to fix at source)

## snapshots 📁
- Take a picture of a table at a moment in time
- Userful to track the history of a column that overwrites itself

## tests 📁
- A place to put assertions in SQL format
- A place for singular tests
- If this SQL command returns more than 0 rows, the dbt build fails

## models 📁
- dbt suggests 3 subfolders:
### staging
- Sources (so raw table from database)
- Staging files are 1 to 1 copy of your data with minimal cleaning steps
    - Data types
    - Renaming columns
    - (Sometimes) Removing columns that you don't use, Standardized...
        - But recommended to maintain 1:1 column structure
### intermediate
- Anything that is not raw nor you want to expose
- No guidelines, just nice for heavy duty cleaning or complex logic

### marts
- If it is in marts, it is ready for consumption
- Tables ready for dashboards
- Properly modeled, clean tables (usually star schema)