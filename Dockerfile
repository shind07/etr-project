# Use an official Python runtime as the base image
FROM python:3.9-slim

# Set the working directory in the container
WORKDIR /app

# Copy the requirements file and the Python script into the container
COPY requirements.txt .
COPY script.py .

# Install the required packages
RUN pip install --no-cache-dir -r requirements.txt

# Run the script when the container launches
CMD ["python", "script.py"]
