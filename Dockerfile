# Start with the official Apache Airflow image
FROM apache/airflow:2.7.1-python3.9

# Set the working directory
WORKDIR /opt/airflow

# Install system dependencies required for building Python packages
USER root
RUN apt-get update && apt-get install -y \
    build-essential \
    gcc \
    python3-dev \
    libpq-dev \
    && apt-get clean

# Copy the requirements.txt into the container
COPY requirements.txt /opt/airflow/requirements.txt

# Install Python dependencies from requirements.txt
USER airflow
#RUN pip install --no-cache-dir -r /opt/airflow/requirements.txt
# Upgrade pip and install dependencies
RUN pip install --upgrade pip setuptools wheel
RUN pip install praw

#RUN pip install --no-cache-dir -r requirements.txt
# Expose necessary ports (Airflow UI)
EXPOSE 8081

# Set the default command
CMD ["airflow", "webserver"]
