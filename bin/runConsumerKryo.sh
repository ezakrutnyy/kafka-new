java -cp target/producer-1.0-SNAPSHOT.jar -Dlog4j.configuration=file:src/main/resources/log4j-consumer.properties kafka.consumer.KryoConsumerRun "$@"