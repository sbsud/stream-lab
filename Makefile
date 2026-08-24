up:
	docker compose --env-file .env -f platform/docker-compose.yaml up -d
down:	
	docker compose --env-file .env -f platform/docker-compose.yaml down -v
load:	
	echo "to be implemented"
vwap:
	echo "to be implemented"