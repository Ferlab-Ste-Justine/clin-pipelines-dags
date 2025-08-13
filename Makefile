.ONESHELL:

setup:
	cp -np .env.sample .env || :

setup_localstack:
	cp -np .env.localstack .env || :

start:
	docker compose --profile minio -f docker-compose-base.yaml up -d --build

# Setup without minio (for using a local minio)
start_base:
	docker compose -f docker-compose-base.yaml up -d --build
