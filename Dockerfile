# Use an official Python runtime as the base image
FROM python:3.9-slim

# Set the working directory in the container
WORKDIR /app

# Copy the requirements file and the Python script into the container
COPY requirements.txt .

# Install the required packages
RUN pip install --no-cache-dir -r requirements.txt

# Copy all Python files
COPY pipeline pipeline/

# Run the script when the container launches
CMD ["python", "pipeline/main.py"]
