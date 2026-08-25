COMPOSE = docker compose --env-file .env -f platform/docker-compose.yaml

up:
	$(COMPOSE) up -d $(S)

down:
	$(COMPOSE) down $(S)

nuke:
	$(COMPOSE) down -v

stop:
	$(COMPOSE) stop $(S)

logs:
	$(COMPOSE) logs -f $(S)

ps:
	$(COMPOSE) ps
load:	
	echo "to be implemented"
vwap:
	echo "to be implemented"