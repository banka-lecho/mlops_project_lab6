# Lab 5: PySpark KMeans на Open Food Facts

Кластеризация продуктов Open Food Facts по пищевой ценности (на 100г) с помощью Spark ML KMeans.

## Установка

Нужна Java (Spark — JVM-приложение):

```bash
brew install openjdk@17
export JAVA_HOME=/opt/homebrew/opt/openjdk@17
```

Виртуальное окружение и зависимости:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Положите сырой датасет в `data/en.openfoodfacts.org.products.csv` (путь задаётся в `src/config.json` → `data.raw_path`).

## Проверка Spark (WordCount)

```bash
python tests/wordcount.py tests/data/sample.txt
```

## Конфигурация

Все настройки — в [`src/config.json`](src/config.json): пути к данным/артефактам (`data`), какие колонки брать (`features`), допуски при очистке (`cleaning`), как считается размер сэмпла (`sampling`), параметры модели (`model`, включая диапазон `k`). Ресурсы машины (ядра, RAM) не задаются вручную — определяются в рантайме (`src/spark_session.py`), конфиг лишь ограничивает, сколько от них брать.

## Запуск

```bash
# 1. Предобработка: сырой CSV -> отбор колонок -> очистка -> сэмпл -> parquet
python src/preprocess.py

# 2. Обучение: подбор k по silhouette, сохранение модели/скейлера/отчёта
python src/main.py train

# 3. Инференс на уже обученной модели
python src/main.py predict --output data/processed/predictions.parquet
```

Результаты:
- `data/interim/products.parquet`, `data/processed/sample.parquet` — данные после `preprocess.py`
- `reports/preprocess_report.json` — сколько строк отсеялось на каждом шаге очистки
- `models/kmeans`, `models/scaler` — обученные артефакты
- `reports/model_report.json` — silhouette по всем k, лучший k, размеры и центры кластеров
- `data/processed/predictions.parquet` — номер кластера для каждого продукта
