.PHONY: up down benchmark logs

up:
	docker-compose -f docker/docker-compose.yml up -d
	@echo "Starting producer, Spark, and Flask..."
	python producer/producer.py &
	python streaming/spark_stream.py &
	python api/app.py

down:
	docker-compose -f docker/docker-compose.yml down

benchmark:
	python benchmark/evaluate.py

logs:
	docker-compose -f docker/docker-compose.yml logs -f
