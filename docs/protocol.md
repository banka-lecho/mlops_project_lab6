# Протокол


## 1. Стороны и роли. 
Модель — активная сторона (всегда инициатор), источник — пассивная.

## 2. Транспорт. 
TDS поверх TCP/1433, MS JDBC Driver 12.x. Строка подключения обязательно с encrypt=true;trustServerCertificate=true;loginTimeout=30.

## 3. Аутентификация. 
SQL Auth, учётка из env-переменных MSSQL_USER / MSSQL_PASSWORD.

## 4. Единица взаимодействия — run. 
UUID, создаётся до выгрузки, закрывается после загрузки.

## 5. формат хранения данных.
Таблица products
Таблица model_runs
Таблица predictions

## 6. Протокол взаимодействия между моделью и источником данных.
1. *start_run*: модель идет в источник и делает pymssql INSERT в ml.model_runs.
2. *fetch_training_data*: источник идет в модель (Spark JDBC), N партиций по product_id	SELECT из raw.products.
3. *save_predictions*: модель идет в источник	(Spark JDBC), append, batchsize	INSERT в ml.predictions.
4. *save_run_metrics*: модель идет в источник	и делает pymssql INSERT в соответствующие таблицы.
5. *finish_run*: модель идет в источник	и даелает pymssql	UPDATE статуса на SUCCESS / FAILED