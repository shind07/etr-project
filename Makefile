.PHONY: build run clean shell test

# Docker image name
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