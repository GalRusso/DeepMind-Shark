# ActiveFence Delivery Data Platform

## Prerequisites

Before you begin, ensure you have the following installed:

- **Python 3.12+**: [Download from python.org](https://www.python.org/downloads/)
- **UV**: Fast Python package installer and resolver - [Installation guide](https://docs.astral.sh/uv/getting-started/installation/)
- **Databricks CLI**: [Installation guide](https://docs.databricks.com/dev-tools/cli/databricks-cli.html)
- **Databricks Connect**: [Installation guide](https://docs.databricks.com/en/dev-tools/databricks-connect.html)

## Getting Started

### 1. Clone and Setup the Project

```bash
# Clone the repository
git clone <repository-url>
cd af-delivery-data-platform
```

### 2. Configure Databricks

Authenticate to your Databricks workspace using the Databricks VS Code Extension with OAuth:

1. Install the [Databricks VS Code Extension](https://marketplace.visualstudio.com/items?itemName=databricks.databricks-vscode).
2. Open the Command Palette in VS Code (`Cmd+Shift+P` or `Ctrl+Shift+P`).
3. Search for and select `Databricks: Add Workspace`.
4. Choose **Sign in with your browser (OAuth)** and follow the prompts to authenticate.

This will securely connect your VS Code environment to your Databricks workspace using OAuth, without needing to manually manage tokens.

### 3. Creating a New Project Using the Template

The repository includes a Databricks Asset Bundle (DAB) template that you can use to quickly scaffold new data pipeline projects. The template provides a standardized structure with all necessary configurations.

To create a new project using the template:

1. Navigate to your desired project directory
2. Initialize a new project using the template:
   ```bash
   databricks bundle init dab-template
   ```
3. Answer the following prompts:
   - Project name (must start with a lowercase letter and contain only lowercase letters, numbers, underscores and hyphens)
   - Your email address (@activefence.com)
   - Unity Catalog name prefix (default: af_delivery)
   - Databricks workspace host URL

The template will create a new project with:
- Pre-configured `databricks.yml` for bundle settings with:
  - Development and production deployment targets
  - Workspace-specific configurations
  - Run-as user settings for production
- Initialized `pyproject.toml` with dependencies
- Standard project structure:
  ```
  your-project/
  ├── databricks.yml        # Bundle configuration
  ├── pyproject.toml       # Python project configuration
  ├── README.md           # Project documentation
  ├── resources/         # Databricks resources (jobs, pipelines)
  └── src/              # Source code and notebooks
  ```

You can then customize the generated project structure according to your specific needs. The template follows Databricks best practices for asset organization and deployment.

### 4. Deployment

#### Local Deployment

To deploy a dev (remote development) copy of this project:

```bash
# Deploy to development environment
databricks bundle deploy --target dev
```

This deploys everything defined for this project to your development workspace. You can find the deployed resources (jobs, pipelines, etc.) in your Databricks workspace under **Workflows**.

#### Development and Production Deployment

You are not allowed to deploy to production manually. Development/Production deployments are handled automatically via the CI/CD pipeline.

### 5. Running Jobs and Pipelines on dev environment

```bash
# Run a job or pipeline
databricks bundle run -t dev --profile <your-profile-name>

# Run a specific job
databricks bundle run -t dev --profile <your-profile-name> --job <job-name>
```

### 6. Development Tools (Optional)

#### VS Code Extension

Install the Databricks extension for Visual Studio Code for enhanced local development:
- [Databricks VS Code Extension](https://docs.databricks.com/dev-tools/vscode-ext.html)

This extension can:
- Configure your virtual environment automatically
- Set up Databricks Connect for running unit tests locally
- Provide IntelliSense for Databricks APIs

#### Databricks Connect

For manual setup of Databricks Connect (if not using VS Code extension):

```bash
# Install databricks-connect (uncomment in pyproject.toml if needed)
pip install "databricks-connect>=16.4"
```

See the [Databricks Connect documentation](https://docs.databricks.com/en/dev-tools/databricks-connect/python/index.html) for detailed setup instructions.

### 7. Project Structure

```
af-delivery-data-platform/
├── dab-template/                # Project template for new data pipelines
│   ├── databricks_template_schema.json  # Template configuration schema
│   └── template/                # Template files and structure
│       └── {{.project_name}}/   # Project-specific template files
│           ├── databricks.yml.tmpl
│           ├── pyproject.toml.tmpl
│           ├── README.md.tmpl
│           ├── resources/
│           └── src/
├── src/                          # Source code
│   ├── af_delivery_data_platform/
│   │   ├── __init__.py
│   │   └── utils.py
│   ├── data_ingestion/           # Data ingestion modules
│   ├── data_transformation/      # Data transformation modules
│   └── sample/                   # Sample code and notebooks
├── resources/                    # Databricks resources
└── databricks.yml               # Databricks bundle configuration
```

### 8. Documentation

- [Databricks Asset Bundles](https://docs.databricks.com/dev-tools/bundles/index.html)
- [Databricks CLI](https://docs.databricks.com/dev-tools/cli/databricks-cli.html)
- [Databricks Connect](https://docs.databricks.com/en/dev-tools/databricks-connect/python/index.html)

## Troubleshooting

### Getting Help

- Check the [Databricks documentation](https://docs.databricks.com/)
- Contact hoangl@activefence.com