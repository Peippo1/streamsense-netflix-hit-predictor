# streamsense-netflix-hit-predictor

## Working with Databricks & GitHub

This project is designed to be developed and run on **Databricks Free Edition**, with **GitHub** as the source of truth for all code and notebooks.

### Prerequisites

- Databricks Free Edition account
- Access to this GitHub repository
- A small all-purpose cluster (DBR runtime with Python + SQL)

### Databricks Free Edition Setup (Serverless)

The Databricks Free Edition uses **serverless compute**, so you do not need to create or attach clusters manually.

To confirm everything is ready:

1. Open your workspace.
2. Create a new notebook (Python or SQL).
3. Run a simple test cell:
   ```python
   print("Databricks serverless compute is running!")

### 1. Connect Databricks to GitHub

1. In the Databricks UI, click your **user icon → Settings**.
2. Go to **Git integration / Linked accounts**.
3. Add a Git credential for **GitHub**:
   - Either authenticate with the Databricks GitHub App (OAuth), or
   - Create a **Personal Access Token** (PAT) in GitHub with `repo` permissions and paste it here.
4. Save your credentials.

> Databricks Git folders (formerly Repos) will use these credentials for all Git operations (clone, pull, push, branch management).

### 2. Clone this repo into Databricks

1. In the left sidebar, click **Workspace**.
2. Click **New → Git folder** (or **Repo** depending on UI).
3. For the **Git URL**, paste:

   ```text
   https://github.com/Peippo1/streamsense-netflix-hit-predictor.git