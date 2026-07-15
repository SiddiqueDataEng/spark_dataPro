# ml/models.py
"""
ML Models
=========
Four models trained on the feature sets from feature_engineering.py:

  1. Customer Churn Classifier     — GradientBoosting (binary)
  2. Customer LTV Regressor        — GradientBoosting (regression)
  3. Product Demand Forecaster     — RandomForest (regression)
  4. Customer Segmentation         — KMeans clustering (unsupervised)

Each model:
  - Performs train/test split (stratified where applicable)
  - Applies preprocessing (scaling, encoding)
  - Hyperparameter tunes with GridSearchCV (light grid for speed)
  - Evaluates on held-out test set
  - Logs params, metrics, and the model artifact to MLflow
  - Prints a human-readable summary
"""

from __future__ import annotations

import warnings
import numpy as np
import pandas as pd
import mlflow
import mlflow.sklearn
from pathlib import Path

from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler, RobustScaler
from sklearn.impute import SimpleImputer
from sklearn.model_selection import train_test_split, GridSearchCV, cross_val_score
from sklearn.ensemble import GradientBoostingClassifier, GradientBoostingRegressor, RandomForestRegressor
from sklearn.cluster import KMeans
from sklearn.metrics import (
    accuracy_score, precision_score, recall_score, f1_score,
    roc_auc_score, classification_report,
    mean_absolute_error, mean_squared_error, r2_score,
    silhouette_score,
)
from sklearn.inspection import permutation_importance

warnings.filterwarnings("ignore")

# MLflow tracking URI (local)
MLFLOW_DIR = Path(__file__).resolve().parent.parent / "mlruns"
mlflow.set_tracking_uri(f"file://{MLFLOW_DIR}")


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _header(title: str):
    print(f"\n{'═'*60}\n  {title}\n{'═'*60}")


def _feature_importance_summary(model, feature_names: list[str], top_n: int = 10):
    """Print top N feature importances from a tree-based model."""
    try:
        importances = model.feature_importances_
        idx = np.argsort(importances)[::-1][:top_n]
        print(f"\n  Top {top_n} Feature Importances:")
        for rank, i in enumerate(idx, 1):
            print(f"    {rank:2d}. {feature_names[i]:<35s}  {importances[i]:.4f}")
    except AttributeError:
        pass


# ─────────────────────────────────────────────────────────────────────────────
# 1. Customer Churn Classifier
# ─────────────────────────────────────────────────────────────────────────────

def train_churn_model(df: pd.DataFrame) -> dict:
    """
    Binary classifier: predict whether a customer has churned
    (recency > 90 days).

    Returns dict of evaluation metrics.
    """
    _header("Model 1 — Customer Churn Classifier")

    TARGET  = "is_churned"
    DROP    = ["customer_id", "is_churned", "lifetime_value"]
    FEATURE_COLS = [c for c in df.columns if c not in DROP]

    X = df[FEATURE_COLS].astype(float)
    y = df[TARGET].astype(int)

    churn_rate = y.mean()
    print(f"  Dataset: {len(df)} customers  |  Churn rate: {churn_rate:.1%}")

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=y
    )

    pipe = Pipeline([
        ("imputer", SimpleImputer(strategy="median")),
        ("scaler",  RobustScaler()),
        ("clf",     GradientBoostingClassifier(random_state=42)),
    ])

    param_grid = {
        "clf__n_estimators":  [100, 200],
        "clf__max_depth":     [3, 4],
        "clf__learning_rate": [0.05, 0.1],
    }

    gs = GridSearchCV(pipe, param_grid, cv=3, scoring="roc_auc",
                      n_jobs=-1, verbose=0)
    gs.fit(X_train, y_train)
    best = gs.best_estimator_

    y_pred  = best.predict(X_test)
    y_proba = best.predict_proba(X_test)[:, 1]

    metrics = {
        "accuracy":  accuracy_score(y_test, y_pred),
        "precision": precision_score(y_test, y_pred, zero_division=0),
        "recall":    recall_score(y_test, y_pred, zero_division=0),
        "f1":        f1_score(y_test, y_pred, zero_division=0),
        "roc_auc":   roc_auc_score(y_test, y_proba),
    }

    print(f"\n  Best params : {gs.best_params_}")
    print(f"  Accuracy    : {metrics['accuracy']:.4f}")
    print(f"  Precision   : {metrics['precision']:.4f}")
    print(f"  Recall      : {metrics['recall']:.4f}")
    print(f"  F1          : {metrics['f1']:.4f}")
    print(f"  ROC-AUC     : {metrics['roc_auc']:.4f}")
    print("\n  Classification Report:")
    print(classification_report(y_test, y_pred,
                                 target_names=["Active", "Churned"]))

    clf_step = best.named_steps["clf"]
    _feature_importance_summary(clf_step, FEATURE_COLS)

    # MLflow logging
    mlflow.set_experiment("churn_classifier")
    with mlflow.start_run(run_name="GBT_churn"):
        mlflow.log_params(gs.best_params_)
        mlflow.log_metrics(metrics)
        mlflow.sklearn.log_model(best, "churn_model")
        print("\n  📦 Logged to MLflow: churn_classifier")

    return metrics


# ─────────────────────────────────────────────────────────────────────────────
# 2. Customer LTV Regressor
# ─────────────────────────────────────────────────────────────────────────────

def train_ltv_model(df: pd.DataFrame) -> dict:
    """
    Gradient Boosting regressor predicting total customer lifetime value.

    Returns dict of evaluation metrics.
    """
    _header("Model 2 — Customer LTV Regressor")

    TARGET  = "lifetime_value"
    DROP    = ["customer_id", "is_churned", "lifetime_value", "monetary"]
    FEATURE_COLS = [c for c in df.columns if c not in DROP]

    X = df[FEATURE_COLS].astype(float)
    y = df[TARGET].astype(float)

    print(f"  Dataset: {len(df)} customers  |  LTV mean: £{y.mean():.2f}  std: £{y.std():.2f}")

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42
    )

    pipe = Pipeline([
        ("imputer", SimpleImputer(strategy="median")),
        ("scaler",  RobustScaler()),
        ("reg",     GradientBoostingRegressor(random_state=42)),
    ])

    param_grid = {
        "reg__n_estimators":  [100, 200],
        "reg__max_depth":     [3, 4],
        "reg__learning_rate": [0.05, 0.1],
    }

    gs = GridSearchCV(pipe, param_grid, cv=3, scoring="r2",
                      n_jobs=-1, verbose=0)
    gs.fit(X_train, y_train)
    best = gs.best_estimator_

    y_pred = best.predict(X_test)

    metrics = {
        "mae":  mean_absolute_error(y_test, y_pred),
        "rmse": np.sqrt(mean_squared_error(y_test, y_pred)),
        "r2":   r2_score(y_test, y_pred),
        "mape": float(np.mean(np.abs((y_test - y_pred) / np.maximum(np.abs(y_test), 1e-9))) * 100),
    }

    print(f"\n  Best params : {gs.best_params_}")
    print(f"  MAE         : £{metrics['mae']:.2f}")
    print(f"  RMSE        : £{metrics['rmse']:.2f}")
    print(f"  R²          : {metrics['r2']:.4f}")
    print(f"  MAPE        : {metrics['mape']:.2f}%")

    # Residual summary
    residuals = y_test - y_pred
    print(f"\n  Residual summary  min={residuals.min():.1f}  "
          f"max={residuals.max():.1f}  mean={residuals.mean():.1f}")

    reg_step = best.named_steps["reg"]
    _feature_importance_summary(reg_step, FEATURE_COLS)

    # MLflow logging
    mlflow.set_experiment("ltv_regressor")
    with mlflow.start_run(run_name="GBT_ltv"):
        mlflow.log_params(gs.best_params_)
        mlflow.log_metrics(metrics)
        mlflow.sklearn.log_model(best, "ltv_model")
        print("\n  📦 Logged to MLflow: ltv_regressor")

    return metrics


# ─────────────────────────────────────────────────────────────────────────────
# 3. Product Demand Forecaster
# ─────────────────────────────────────────────────────────────────────────────

def train_demand_model(df: pd.DataFrame) -> dict:
    """
    Random Forest regressor for weekly product demand forecasting.

    Returns dict of evaluation metrics.
    """
    _header("Model 3 — Product Demand Forecaster")

    TARGET  = "units_sold"
    DROP    = ["product_id", "units_sold"]
    FEATURE_COLS = [c for c in df.columns if c not in DROP]

    df_clean = df.dropna(subset=[TARGET])
    X = df_clean[FEATURE_COLS].astype(float)
    y = df_clean[TARGET].astype(float)

    print(f"  Dataset: {len(df_clean)} weekly product records  |  "
          f"Avg weekly units: {y.mean():.1f}")

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42
    )

    pipe = Pipeline([
        ("imputer", SimpleImputer(strategy="median")),
        ("reg",     RandomForestRegressor(random_state=42, n_jobs=-1)),
    ])

    param_grid = {
        "reg__n_estimators": [100, 200],
        "reg__max_depth":    [None, 10],
        "reg__min_samples_leaf": [1, 3],
    }

    gs = GridSearchCV(pipe, param_grid, cv=3, scoring="r2",
                      n_jobs=-1, verbose=0)
    gs.fit(X_train, y_train)
    best = gs.best_estimator_

    y_pred = best.predict(X_test)

    metrics = {
        "mae":  mean_absolute_error(y_test, y_pred),
        "rmse": np.sqrt(mean_squared_error(y_test, y_pred)),
        "r2":   r2_score(y_test, y_pred),
        "mape": float(np.mean(np.abs((y_test - y_pred) / np.maximum(y_test, 1e-9))) * 100),
    }

    print(f"\n  Best params : {gs.best_params_}")
    print(f"  MAE         : {metrics['mae']:.2f} units")
    print(f"  RMSE        : {metrics['rmse']:.2f} units")
    print(f"  R²          : {metrics['r2']:.4f}")
    print(f"  MAPE        : {metrics['mape']:.2f}%")

    rf_step = best.named_steps["reg"]
    _feature_importance_summary(rf_step, FEATURE_COLS)

    # MLflow logging
    mlflow.set_experiment("demand_forecaster")
    with mlflow.start_run(run_name="RF_demand"):
        mlflow.log_params(gs.best_params_)
        mlflow.log_metrics(metrics)
        mlflow.sklearn.log_model(best, "demand_model")
        print("\n  📦 Logged to MLflow: demand_forecaster")

    return metrics


# ─────────────────────────────────────────────────────────────────────────────
# 4. Customer Segmentation (KMeans)
# ─────────────────────────────────────────────────────────────────────────────

def train_segmentation_model(df: pd.DataFrame) -> dict:
    """
    KMeans clustering for customer segmentation (k=4..8, elbow method).
    Features: recency_days, frequency, monetary, avg_discount, tenure_days.

    Returns dict with silhouette score and cluster summary.
    """
    _header("Model 4 — Customer Segmentation (KMeans)")

    SEG_FEATURES = [
        "recency_days", "frequency", "monetary",
        "avg_discount", "tenure_days", "distinct_categories",
    ]
    available = [f for f in SEG_FEATURES if f in df.columns]
    X = df[available].astype(float).fillna(0)

    print(f"  Dataset: {len(X)} customers  |  Features: {available}")

    scaler = RobustScaler()
    X_scaled = scaler.fit_transform(X)

    # Elbow: try k=3..8, pick best silhouette
    best_k, best_sil, best_km = 4, -1, None
    print("\n  Elbow search:")
    for k in range(3, 9):
        km = KMeans(n_clusters=k, random_state=42, n_init=10)
        labels = km.fit_predict(X_scaled)
        sil = silhouette_score(X_scaled, labels, sample_size=min(2000, len(X_scaled)))
        print(f"    k={k}  silhouette={sil:.4f}")
        if sil > best_sil:
            best_k, best_sil, best_km = k, sil, km

    print(f"\n  Best k: {best_k}  (silhouette={best_sil:.4f})")
    df = df.copy()
    df["cluster"] = best_km.labels_

    # Cluster profiling
    profile = df.groupby("cluster")[available].mean().round(2)
    df["cluster_size"] = df.groupby("cluster")["cluster"].transform("count")

    print("\n  Cluster Profiles:")
    print(profile.to_string())
    print("\n  Cluster Sizes:")
    print(df.groupby("cluster").size().rename("customers").to_string())

    # Assign business labels based on RFM heuristics
    profile_sorted = profile.sort_values("monetary", ascending=False)
    labels_map = {}
    label_list = ["High-Value", "Loyal", "At-Risk", "Dormant",
                  "Occasional", "New", "Bargain Hunters", "Premium"]
    for i, cluster_id in enumerate(profile_sorted.index):
        labels_map[cluster_id] = label_list[i] if i < len(label_list) else f"Cluster-{cluster_id}"

    df["segment_label"] = df["cluster"].map(labels_map)
    segment_summary = df.groupby("segment_label").agg(
        customers=("cluster", "count"),
        avg_monetary=("monetary", "mean"),
        avg_recency=("recency_days", "mean"),
        avg_frequency=("frequency", "mean"),
    ).round(2).sort_values("avg_monetary", ascending=False)

    print("\n  Segment Summary:")
    print(segment_summary.to_string())

    metrics = {
        "best_k": float(best_k),
        "silhouette_score": float(best_sil),
    }

    # MLflow logging
    mlflow.set_experiment("customer_segmentation")
    with mlflow.start_run(run_name=f"KMeans_k{best_k}"):
        mlflow.log_param("k", best_k)
        mlflow.log_param("features", str(available))
        mlflow.log_metrics({"silhouette_score": best_sil})
        mlflow.sklearn.log_model(best_km, "segmentation_model")
        print(f"\n  📦 Logged to MLflow: customer_segmentation (k={best_k})")

    return metrics
