IMAGE_NAME = etr-project

build:
	docker build -t $(IMAGE_NAME) .

run:
	docker run -v $(PWD)/data:/app/data $(IMAGE_NAME)

shell:
	docker run -it --rm -v $(PWD)/data:/app/data $(IMAGE_NAME) /bin/bash -c "cd /app && bash"

