FROM eclipse-temurin:17-jre-focal

ADD https://downloads.apache.org/kafka/3.6.2/kafka_2.13-3.6.2.tgz kafka.tgz

RUN tar -xvzf kafka.tgz

RUN rm kafka.tgz

RUN mv /kafka_2.13-3.6.2 /kafka

ENV PATH=${PATH}:/kafka/bin

WORKDIR learning

ADD runner.sh runner.sh

CMD [ "sh", "runner.sh" ]