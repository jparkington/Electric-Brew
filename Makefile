# This Makefile utilizes Conda for Python package management and environment setup.

# Create Conda environment from environment.yml file
create-env:
	@echo "Creating Conda environment..."
	@conda env create -f environment.yml || echo "Environment already exists."

# Remove Conda environment
remove-env:
	@echo "Removing Conda environment..."
	@conda env remove -n electric-brew

# Create a chain of commands to set up the entire environment and generate queries
setup: create-env
	@echo "Environment setup complete."

# Clean up the environment and created files.
clean: remove-env
	@echo "Cleaning up..."
	rm data/*
	@echo "Environment and data files removed."
