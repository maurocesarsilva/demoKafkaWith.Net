# Comandos Docker
Criar rede:
- docker network create app

 Abrir terminal interativo
- docker exec -it 72dc1cbe1c3c /bin/bash
- 72dc1cbe1c3c  = id do container

# Comandos Kafka
Criar topico:
- kafka-topics --create --topic my-topic --partitions 3 --replication-factor 1 --zookeeper localhost:2181


listar todos os topicos:
- kafka-topics --list --bootstrap-server localhost:9092


produzir mensagens:
- kafka-console-producer --topic my-topic --broker-list localhost:9092

consumir mensagens:
- kafka-console-consumer --topic my-topic --from-beginning --bootstrap-server localhost:9092


# Instalar apache avro
- dotnet tool install --global Apache.Avro.Tools
- doc: https://avro.apache.org/docs/1.11.1/specification/_print/

# Dependencias
- Confluent.Kafka
- Confluent.SchemaRegistry.Serdes.Avro

