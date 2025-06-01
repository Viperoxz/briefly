.PHONY: help dagster test lint clean

help:
	@echo "Dagster Pipeline Development Makefile"
	@echo ""
	@echo "Available targets:"
	@echo "  dagster    Run Dagster dev server (UI + daemon)"
	@echo "  test       Run pytest on the pipeline"
	@echo "  lint       Run flake8 for linting"
	@echo "  clean      Remove Python cache and build artifacts"

dagster:
	dagster dev

test:
	pytest tests/

lint:
	flake8 src/ tests/

clean:
	find . -type d -name "__pycache__" -exec rm -rf {} +
	find . -type d -name ".pytest_cache" -exec rm -rf {} +
	rm -rf .mypy_cache .coverage htmlcov