.PHONY: build run clean shell test

AWS_REGION = us-east-1
ECR_REPO = $(shell terraform -chdir=terraform output -raw ecr_repository_url)
S3_BUCKET = $(shell terraform -chdir=terraform output -raw s3_bucket_name)
IMAGE_NAME = etr-project

# Build the Docker image
build:
	docker build -t $(IMAGE_NAME) .

# Run the container
run:
	docker run -v $(PWD)/data:/app/data $(IMAGE_NAME)

# Build and run in one command
all: build run

# Clean up Docker images
clean:
	docker rmi $(IMAGE_NAME)

# Shell into the container's data directory
shell:
	docker run -it --rm -v $(PWD)/data:/app/data $(IMAGE_NAME) /bin/bash -c "cd /app && bash"

# Run tests in the container
test:
	docker run --rm $(IMAGE_NAME) pytest -v test_script.py

ecr-login:
	aws ecr get-login-password --region $(AWS_REGION) | docker login --username AWS --password-stdin $(shell echo $(ECR_REPO) | cut -d'/' -f1)

push: build ecr-login
	docker tag $(IMAGE_NAME):latest $(ECR_REPO):latest
	docker push $(ECR_REPO):latest

upload-data:
	aws s3 cp data/ETR_SimOutput_2024_15.parquet s3://$(S3_BUCKET)/input/
