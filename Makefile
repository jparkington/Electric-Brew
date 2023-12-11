# -----------------------------------------------------------------------------
# Environment Management with Conda
# -----------------------------------------------------------------------------

ENV_NAME := electric-brew

create-env:
	@echo "Creating Conda environment..."
	@conda env create -f environment.yml || echo "Environment already exists."

remove-env:
	@echo "Removing Conda environment..."
	@conda env remove -n $(ENV_NAME)

setup: create-env set-pythonpath
	@echo "Environment setup complete."

set-pythonpath:
	@echo "Setting PYTHONPATH..."
	@conda env config vars set PYTHONPATH="$(PWD)/src" --name $(ENV_NAME)

update-env: set-pythonpath
	@echo "Updating Conda environment..."
	@conda env update -f environment.yml || echo "Failed to update environment."


# -----------------------------------------------------------------------------
# ETL Pipeline
# -----------------------------------------------------------------------------

etl:
	@echo "Initiating the ETL pipeline..."
	@conda run -n $(ENV_NAME) python -B src/utils/etl.py
	@echo "ETL pipeline execution complete. Data is now ready for analytics."


# -----------------------------------------------------------------------------
# Initial Exploratory Data Analysis (EDA)
# -----------------------------------------------------------------------------

eda1:
	@echo "Plotting EDA 1 visuals..."
	@echo "Generating 'Distribution of kWh, Normalized by Year & Meter ID'."
	@conda run -n $(ENV_NAME) python -B src/eda/eda_1_distribution_of_kwh.py
	@echo "Generating 'Max Usage', 'Mean Usage', and 'Percent Difference' charts."
	@conda run -n $(ENV_NAME) python -B src/eda/eda_1_mean_and_max.py
	@echo "Generating 'Count of Energy Spikes by Meter ID'."
	@conda run -n $(ENV_NAME) python -B src/eda/eda_1_count_of_spikes.py
	@echo "Generating 'Count of Energy Spikes by Meter ID & Year'."
	@conda run -n $(ENV_NAME) python -B src/eda/eda_1_spikes_by_year.py

eda2:
	@echo "Plotting EDA 2 visuals..."
	@echo "Generating 'Total kWh Usage by Period'."
	@conda run -n $(ENV_NAME) python -B src/eda/eda_2_kwh_by_period.py
	@echo "Generating 'Average kWh Usage per Hour by Period'."
	@conda run -n $(ENV_NAME) python -B src/eda/eda_2_avg_kwh_by_period.py
	@echo "Generating 'Scatter Plot of kWh Usage Over Time Colored by Meter ID'."
	@conda run -n $(ENV_NAME) python -B src/eda/eda_2_kwh_over_time_by_meter.py
	@echo "Generating 'Scatter Plot of kWh Usage Over Time Colored by Location'."
	@conda run -n $(ENV_NAME) python -B src/eda/eda_2_kwh_by_location.py

# -----------------------------------------------------------------------------
# Peak Hour & Supplier Modeling
# -----------------------------------------------------------------------------

jp01:
	@echo "Visualizing Relationship Between kWh and Total Cost..."
	@conda run -n $(ENV_NAME) python -B src/analysis/jp/jp01.py

jp02:
	@echo "Visualizing Hourly Variation of kWh Usage by Month..."
	@conda run -n $(ENV_NAME) python -B src/analysis/jp/jp02.py

jp03:
	@echo "Visualizing Average Cost by Period Over Time..."
	@conda run -n $(ENV_NAME) python -B src/analysis/jp/jp03.py

jp04:
	@echo "Applying Anomaly Detection Using Isolation Forest..."
	@conda run -n $(ENV_NAME) python -B src/analysis/jp/jp04.py

jp05:
	@echo "Visualizing Heatmap of High Correlations..."
	@conda run -n $(ENV_NAME) python -B src/analysis/jp/jp05.py

jp06:
	@echo "Applying Feature Selection Using LASSO..."
	@conda run -n $(ENV_NAME) python -B src/analysis/jp/jp06.py

jp07:
	@echo "Performing K-Means Clustering and PCA Visualization..."
	@conda run -n $(ENV_NAME) python -B src/analysis/jp/jp07.py

jp08:
	@echo "Fitting Linear Regression Model and Visualizing Results..."
	@conda run -n $(ENV_NAME) python -B src/analysis/jp/jp08.py

jp09:
	@echo "Fitting Random Forest Model and Visualizing Predictions..."
	@conda run -n $(ENV_NAME) python -B src/analysis/jp/jp09.py

jp10:
	@echo "Comparing Cross-Validation R² Scores Across Folds..."
	@conda run -n $(ENV_NAME) python -B src/analysis/jp/jp10.py

jp11:
	@echo "Performing SLSQP Optimization and Visualizing Results..."
	@conda run -n $(ENV_NAME) python -B src/analysis/jp/jp11.py

jp12:
	@echo "Visualizing Percentage Changes in Categorical Values After Optimization..."
	@conda run -n $(ENV_NAME) python -B src/analysis/jp/jp12.py

run-all-jp: jp01 jp02 jp03 jp04 jp05 jp06 jp07 jp08 jp09 jp10 jp11 jp12
	@echo "All scripts for 'Peak Hour & Supplier Modeling' executed."
