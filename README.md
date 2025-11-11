# StreamSense: Netflix Hit Predictor

> "Can we predict whether a Netflix title will be a hit before it’s released?"

---

## Project Overview

StreamSense is an end-to-end data and AI project built entirely on Databricks Free Edition (serverless compute).  
It explores whether it’s possible to predict a Netflix title’s success using only publicly available metadata such as category, rating, duration, release year, and description text.

The project demonstrates a full data-to-insight pipeline:
- Ingest raw data from Kaggle  
- Clean and prepare features using PySpark  
- Train and evaluate a Random Forest model with scikit-learn  
- Track experiments and models using MLflow  
- Deliver interactive What-If predictions and visual insights  

Built to showcase practical, reproducible AI workflows in Databricks, StreamSense highlights how data engineering and machine learning integrate seamlessly on a unified platform.

---

## Working with Databricks & GitHub

This project is designed to be developed and run on Databricks Free Edition, with GitHub as the source of truth for all code and notebooks.

### Prerequisites

- Databricks Free Edition account  
- Access to this GitHub repository  
- (Optional) Local environment with VS Code + Databricks extension for offline editing  

---

### Databricks Free Edition Setup (Serverless)

The Databricks Free Edition uses serverless compute, so you do not need to create or attach clusters manually.

To confirm everything is ready, run a simple test cell:

```python
print("Databricks serverless compute is running!")
```

If it executes successfully, your workspace is ready.

---

### Step 1: Connect Databricks to GitHub

1. In the Databricks UI, click your **user icon → Settings**.  
2. Go to **Git integration / Linked accounts**.  
3. Add a Git credential for GitHub:  
   - Either authenticate with the Databricks GitHub App (OAuth), or  
   - Create a Personal Access Token (PAT) in GitHub with `repo` permissions and paste it here.  
4. Save your credentials.

> Databricks Git folders (formerly Repos) use these credentials for all Git operations (clone, pull, push, branch management).

---

### Step 2: Clone this repo into Databricks

1. In the left sidebar, click **Workspace**.  
2. Click **New → Git folder** (or **Repo**, depending on UI).  
3. For the Git URL, paste:

```
https://github.com/Peippo1/streamsense-netflix-hit-predictor.git
```

4. Select your branch and click **Clone**.

---

## Notebooks Overview

| Notebook | Name | Description |
|-----------|------|-------------|
| 01 | `data_ingestion_exploration` | Loads the raw Netflix dataset, explores schema, inspects nulls, and saves as `netflix_raw` Delta table. |
| 02 | `feature_engineering_and_labels` | Cleans column names, extracts features (release year, duration, category), and creates a heuristic `is_hit` label. Saves processed data as `netflix_clean`. |
| 03 | `modelling_and_evaluation` | Trains a baseline Random Forest classifier, evaluates metrics (accuracy, ROC-AUC, F1), and logs results with MLflow including signature and input schema. |
| 04 | `hit_predictor_demo` | Loads the tracked model, enables interactive What-If predictions for hypothetical titles, and visualises hit patterns by category, rating, and release year. |

---

## Tech Stack

- Platform: Databricks (Free Edition, serverless compute)  
- Languages: Python (PySpark + scikit-learn)  
- Tracking: MLflow  
- Data Source: [Netflix Titles Dataset — Kaggle](https://www.kaggle.com/datasets/rohitgrewal/netflix-data)  
- Visualisation: Databricks `display()` API + matplotlib  

---

## Data Source & License

This project uses the "Netflix Data" dataset by Rohit Grewal on Kaggle.

- Dataset: [https://www.kaggle.com/datasets/rohitgrewal/netflix-data](https://www.kaggle.com/datasets/rohitgrewal/netflix-data)  
- License (as listed on Kaggle): Open Database License (ODbL)-style terms  

The raw dataset is not included in this repository.  
To reproduce results, download it from Kaggle and upload it to your own Databricks workspace under:  
`/Volumes/workspace/my_catalog/my_volume/Netflix Dataset.csv`

---

## Usage

To reproduce or explore StreamSense:

1. Clone this repository into Databricks as a Git folder.
2. Download the Netflix dataset from [Kaggle](https://www.kaggle.com/datasets/rohitgrewal/netflix-data) and upload it to your workspace under:
   ```
   /Volumes/workspace/my_catalog/my_volume/Netflix Dataset.csv
   ```
3. Run the notebooks in order:

   1. `01_data_ingestion_exploration`
   2. `02_feature_engineering_and_labels`
   3. `03_modelling_and_evaluation`
   4. `04_hit_predictor_demo`

Each notebook builds upon the previous one to take you from raw data to an interactive model demo.

### Example: What-If prediction

In `04_hit_predictor_demo`, you can generate a prediction for a new, hypothetical title using:

```python
predict_hit_probability(
    category="Movie",
    rating="TV-MA",
    release_year=2024,
    duration_num=110,
    is_movie=1,
    country="United States"
)
```

Example output:
```
Predicted hit probability: 0.76
```

---

## Results & Insights

The baseline Random Forest model achieves balanced performance across metrics and reveals several patterns in the dataset:

| Metric | Score |
|---------|--------|
| Accuracy | ~0.78 |
| Precision | ~0.75 |
| Recall | ~0.79 |
| F1-Score | ~0.77 |
| ROC-AUC | ~0.80 |

### Key observations

- **Movies released after 2020** show a slightly higher predicted hit probability, likely due to modern content patterns.  
- **TV-MA-rated titles** trend toward stronger hit potential compared to family-rated titles.  
- **Feature importance analysis** highlights:
  - `description_length` (proxy for content richness)
  - `release_year`
  - `rating`
  - `category`
  as primary predictors of success.

### Visual summary

Notebook 04 includes visual breakdowns such as:
- Hit rate by release year (trend line)
- Hit rate by rating category
- Feature importance ranking

Optionally, these can be converted into a Databricks dashboard view for presentation or submission.

---

## Final Note

This project was developed for the Databricks Free Edition Hackathon, showcasing what’s possible with open data and accessible cloud AI tooling.  
It’s designed to be fully portable and reproducible — requiring no paid compute, just a free Databricks account.