# Use the official Airflow image as a parent image
FROM apache/airflow:2.8.4-python3.10
# Install the Python packages specified in requirements.txt
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt