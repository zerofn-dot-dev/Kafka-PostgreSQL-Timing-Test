TIMES=10000

for ((i = 1; i <= TIMES; i++)); do
  ./kafka_connect_psql_testing -mode delete;
  ./kafka_connect_psql_testing -mode create; \
  ./kafka_connect_psql_testing -mode run && \
  sleep 0.1
done
