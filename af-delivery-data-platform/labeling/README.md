# labeling

This project was generated using the DISMIS Trends Template, which follows Databricks Asset Bundle best practices.

## Project Structure

```
labeling/
├── databricks.yml        # Databricks Asset Bundle configuration
├── pyproject.toml       # Python project configuration
├── resources/          # Pipeline and job configurations
│   ├── *.pipeline.yml  # DLT pipeline configurations
│   └── *.job.yml       # Databricks job configurations
└── src/               # Source code
    ├── data_ingestion/    # Data ingestion notebooks
    └── data_transformation/ # Data transformation notebooks
```

## Development Setup

1. Create a Python virtual environment:
   ```bash
   python -m venv .venv
   source .venv/bin/activate  # On Windows: .venv\Scripts\activate
   pip install -e ".[dev]"
   ```

2. Configure your Databricks CLI:
   ```bash
   databricks configure --profile DEFAULT --host https://dbc-1239d0c6-d339.cloud.databricks.com
   ```

3. Deploy the bundle to development:
   ```bash
   databricks bundle deploy
   ```

## Environment Configuration

- Development Catalog: `af_delivery_dev`
- Production Catalog: `af_delivery_prod`
- Workspace URL: `https://dbc-1239d0c6-d339.cloud.databricks.com`

## Contributing

1. Create a new branch for your changes
2. Make your changes and test them locally
3. Deploy to development environment for testing
4. Create a pull request for review

## Contact

For questions or issues, contact datn@activefence.com
