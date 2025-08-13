.ONESHELL:

setup:
	cp -np .env.localstack .env || :

start:
	docker compose -f docker-compose-localstack.yaml up -d --build