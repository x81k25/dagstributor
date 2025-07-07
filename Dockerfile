FROM dagster/dagster-k8s:1.10.20

# Install additional dependencies if needed
RUN pip install --no-cache-dir dagster-postgres==0.21.0 pyyaml

# Copy requirements and install Python dependencies first
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt || true

# Copy the dagstributor code
WORKDIR /opt/dagster/app
COPY . ./dagstributor/

# Copy our custom dagster.yaml to the default location
RUN mkdir -p /opt/dagster/dagster_home && \
    cp /opt/dagster/app/dagstributor/config/dagster.yaml /opt/dagster/dagster_home/dagster.yaml

# Set Python path
ENV PYTHONPATH="/opt/dagster/app/dagstributor:${PYTHONPATH}"

# Set working directory
WORKDIR /opt/dagster/app/dagstributor