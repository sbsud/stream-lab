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

create_topic:
	$(COMPOSE) exec broker kafka-topics --bootstrap-server broker:29092 --create --topic $(S) --partitions 4 --replication-factor 1

delete_topic:
	$(COMPOSE) exec broker kafka-topics --bootstrap-server broker:29092 --delete --topic $(S)

ps:
	$(COMPOSE) ps

load_build:	
	cd loadgen/stream-lab-producer && mvn clean install && cd ../..

load:	
	cd loadgen/stream-lab-producer && date -u && mvn -q compile exec:java -Dexec.mainClass=TradesProducer && date -u && cd ../..


fsql:
	$(COMPOSE) exec jobmanager ./bin/sql-client.sh -f /opt/flink/jobs/event-time-vwap/$(S)

fsqli:
	$(COMPOSE) exec jobmanager ./bin/sql-client.sh