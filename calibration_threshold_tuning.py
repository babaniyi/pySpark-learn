''' 
New approach for handlig class imbalance that takes a combination of `threshold tuning`, `conformal prediction`, `CatBoost`
Below is an example of implementing the proposed approach for dealing with class imbalance in a machine learning classification problem.
'''

# pip install numpy pandas scikit-learn catboost xgboost


import numpy as np
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score, classification_report, confusion_matrix
from catboost import CatBoostClassifier
from xgboost import XGBClassifier
from sklearn.calibration import CalibratedClassifierCV

# Generate a sample dataset (replace this with your actual dataset)
np.random.seed(42)
X = np.random.rand(1000, 10)
y = np.random.choice([0, 1], size=1000, p=[0.9, 0.1])

# Create a DataFrame
columns = [f"feature_{i}" for i in range(1, 11)]
df = pd.DataFrame(X, columns=columns)
df['target'] = y

# Split the dataset into train and test sets
X_train, X_test, y_train, y_test = train_test_split(df.drop('target', axis=1), df['target'], test_size=0.2, random_state=42)

# Step 1: Train CatBoost model
catboost_model = CatBoostClassifier(iterations=100, random_state=42, verbose=0)
catboost_model.fit(X_train, y_train)

# Step 2: Calibrate CatBoost model
calibrated_catboost_model = CalibratedClassifierCV(catboost_model, method='sigmoid', cv='prefit')
calibrated_catboost_model.fit(X_train, y_train)

# Step 3: Generate conformal prediction intervals using CatBoost
catboost_predictions_proba = calibrated_catboost_model.predict_proba(X_test)[:, 1]

# Step 4: Find an optimal threshold for prediction
thresholds = np.arange(0, 1.05, 0.05)
accuracy_scores = []
for threshold in thresholds:
    y_pred = catboost_predictions_proba > threshold
    accuracy_scores.append(accuracy_score(y_test, y_pred))

optimal_threshold = thresholds[np.argmax(accuracy_scores)]
print(f"Optimal Threshold: {optimal_threshold}")

# Step 5: Make predictions using the optimal threshold
final_catboost_predictions = catboost_predictions_proba > optimal_threshold

# Step 6: Evaluate the final predictions
print("Classification Report:")
print(classification_report(y_test, final_catboost_predictions))
print("Confusion Matrix:")
print(confusion_matrix(y_test, final_catboost_predictions))
