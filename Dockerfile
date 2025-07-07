FROM dagster/dagster-k8s:1.10.20

# Install additional dependencies if needed
RUN pip install --no-cache-dir dagster-postgres==0.21.0

# Copy the dagstributor code
WORKDIR /opt/dagster/app
COPY . ./dagstributor/

# Install the dagstributor package
RUN pip install -e ./dagstributor

# Set working directory
WORKDIR /opt/dagster/app/dagstributor