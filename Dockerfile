# Use the official Airflow image as base
FROM apache/airflow:2.8.2

# Copy the requirements file into the container
COPY requirements.txt .

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt