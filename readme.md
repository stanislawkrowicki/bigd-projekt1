# ParkVisits (21)
## Stanisław Krowicki 160133

## How to copy files?
```shell
docker compose cp ../projekt1 master:/

docker compose exec -it master bash

sudo mv /projekt1 .

```

```sh
hadoop fs -mkdir -p input
hadoop fs -put input/* input
```

## How to run?
```shell
./run_mr.sh input/datasource1 output_mr

./run_hive.sh output_mr input/datasource4 output
```

## Read
```shell
hadoop fs -cat output/*
```