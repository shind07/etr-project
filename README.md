# etr-project

## User Guide

1. Install Docker
2. Copy the ETR_SimOutput_2024_15.parquet into the data directory
3. Build and run the the docker image with `make build run`
4. The output will be written to `data/percentiles_df.parquet`

## Project Structure

- `/pipeline` - python code the pipeline, with `main.py` as the entry point
- `eda.ipynb` - a python notebook to explore the dataset
- `Makefile` - shortcuts for building and runnign the Docker container
- `requirements.txt` - requirements for running the code in either Docker or a venv

## AWS Integration

1. Set up AWS credentials 
2. Install Terraform
3. Create AWS resources: `make apply`
4. Push data to S3 bucket: `make upload-data`
5. Push docker image to ECR: `make push`
6. TBC - run docker container on AWS