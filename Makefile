# This Makefile utilizes Conda for Python package management and environment setup.

# Define the Conda environment name
ENV_NAME := electric-brew

# Dynamically set PYTHONPATH for the current Conda environment
set-pythonpath:
	@echo "Setting PYTHONPATH..."
	@conda env config vars set PYTHONPATH="$(PWD)/src" --name $(ENV_NAME)

# Create Conda environment from environment.yml file
create-env:
	@echo "Creating Conda environment..."
	@conda env create -f environment.yml || echo "Environment already exists."

# Update Conda environment from environment.yml file
update-env: set-pythonpath
	@echo "Updating Conda environment..."
	@conda env update --file environment.yml || echo "Failed to update environment."

# Remove Conda environment
remove-env:
	@echo "Removing Conda environment..."
	@conda env remove -n $(ENV_NAME)

# Run group 1 queries
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

# Create a chain of commands to set up the Conda environment properly
setup: create-env set-pythonpath
	@echo "Environment setup complete."