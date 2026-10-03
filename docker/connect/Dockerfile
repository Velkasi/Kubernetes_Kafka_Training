# Image Kafka Connect du labo : image Confluent + plugin JDBC (source et sink).
# Le plugin JDBC embarque deja le driver PostgreSQL.
# Le convertisseur Avro (io.confluent.connect.avro.AvroConverter) est deja dans l'image de base.
FROM confluentinc/cp-kafka-connect:7.9.10

ARG JDBC_VERSION=10.8.4

RUN confluent-hub install --no-prompt "confluentinc/kafka-connect-jdbc:${JDBC_VERSION}"
