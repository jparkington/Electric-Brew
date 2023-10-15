# This Makefile utilizes Conda for Python package management and environment setup.

# Dynamically set PYTHONPATH for the current Conda environment
set-pythonpath:
	@echo "Setting PYTHONPATH..."
	@conda env config vars set PYTHONPATH="$(PWD)/src" --name electric-brew

# Create Conda environment from environment.yml file
create-env: set-pythonpath
	@echo "Creating Conda environment..."
	@conda env create -f environment.yml || echo "Environment already exists."

# Update Conda environment from environment.yml file
update-env: set-pythonpath
	@echo "Updating Conda environment..."
	@conda env update --file environment.yml || echo "Failed to update environment."

# Remove Conda environment
remove-env:
	@echo "Removing Conda environment..."
	@conda env remove -n electric-brew

# Create a chain of commands to set up the entire environment and generate visuals
setup: create-env
	@echo "Environment setup complete."

# Clean up the environment and created files.
clean: remove-env
	@echo "Cleaning up..."
	@echo "Environment and data files removed."
