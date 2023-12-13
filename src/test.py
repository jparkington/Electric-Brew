import numpy as np
import pandas as pd
from analysis.jp.jp06 import lasso_outputs
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error, r2_score

def extended_slr_analysis(X_train: np.ndarray = lasso_outputs['X_train'], 
                          X_test: np.ndarray = lasso_outputs['X_test'], 
                          y_train: pd.Series = lasso_outputs['y_train'], 
                          y_test: pd.Series = lasso_outputs['y_test']):
    """
    Extends the original slr function to provide additional data for analysis.
    """
    # Fitting the Linear Regression model
    linear_regression = LinearRegression()
    linear_regression.fit(X_train, y_train)
    y_pred = linear_regression.predict(X_test)

    # Calculating R² and MSE
    r2 = r2_score(y_test, y_pred)
    mse = mean_squared_error(y_test, y_pred)

    # Printing R² and MSE
    print(f"R² (Coefficient of Determination): {r2:.3f}")
    print(f"MSE (Mean Squared Error): {mse:.3f}")

    # Additional Analysis
    # 1. Model Coefficients and Intercept
    print("Model Coefficients:", linear_regression.coef_)
    print("Model Intercept:", linear_regression.intercept_)

    # 2. Residual Analysis
    residuals = y_test - y_pred
    print("Residuals Summary:\n", residuals.describe())

    # 3. Distribution of Predictions vs Actual Values
    # This can be helpful to understand the spread and bias in predictions
    print("Predictions Summary:\n", pd.Series(y_pred).describe())
    print("Actual Values Summary:\n", y_test.describe())

    # Optional: Plotting residuals or additional diagnostic plots
    # ...

if __name__ == "__main__":
    extended_slr_analysis()
