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

topic:
	$(COMPOSE) exec broker kafka-topics --bootstrap-server broker:29092 --create --topic $(S) --partitions 4 --replication-factor 1

ps:
	$(COMPOSE) ps
load:	
	cd loadgen/stream-lab-producer && mvn -q compile exec:java -Dexec.mainClass=TradesProducer

	