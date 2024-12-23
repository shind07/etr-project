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

## Decision Journal

### Initial Steps

- Lets get a jupyter notebook going to play around with things and explore the data
- Need to install and set up R
- Run the script in R for a baseline time

## Plan
- First, get it working in python
- Then, make it fast
- Finally, make it pretty
- Move it into Docker so it runs easily
- Set up makefile
- Run it on AWS?

going to skip google sheets for now

## Stats
- R baseline - roughly a minute or so, time by hand
- Python: took about 17 seconds, 9 GB of RAM per Docker


# 3 - improvement suggestions
- RAG approach? LLM?
- google sheets - depends who the end user is? could do a custom ui? effort vs reward?
- pre compute standard queries as part of an etl pipeline

- skipping unit tests - not letting perfect be the enemy of good
treating this like a hackathon wher eI dont get style points

Timing Summary:
Processing time: 11.21 seconds
Google operations time: 1.06 seconds
Total runtime: 1.25 seconds

Timing Summary:
Processing time: 13.06 seconds
Google operations time: 1.38 seconds
Total runtime: 1.59 seconds

takes a similar time, uses docker and python, much more extensible
use spark