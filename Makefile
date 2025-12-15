.PHONY: install run ingest transform lint clean

install: 
	pip install -r requirements.txt

ingest:
	python -m src.etl.ingest_covid

transform:
	python -m src.etl.transform_covid

run: 
	python -m src.etl.ingest_covid && python -m src.etl.transform_covid

clean:
	find . -type d -name "__pycache__" -exec rm -rf {} +


