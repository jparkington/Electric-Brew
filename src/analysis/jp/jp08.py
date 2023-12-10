import matplotlib.pyplot as plt
import numpy  as np
import pandas as pd

from analysis.jp.flat     import prepared_data
from analysis.jp.jp04     import remove_anomalies
from analysis.jp.jp06     import lasso
from sklearn.linear_model import LinearRegression
from sklearn.metrics      import mean_squared_error, r2_score
from utils.runtime        import find_project_root

def slr(X_train : np.ndarray, 
        X_test  : np.ndarray, 
        y_train : pd.Series, 
        y_test  : pd.Series):
    '''
    Fits a Linear Regression model and visualizes predictions against actual values.

    Methodology:
        1. Fit a Linear Regression model using the training data.
        2. Predict values using the test data.
        3. Visualize the predicted vs. actual values and calculate R² and MSE.

    Data Science Concepts:
        • Linear Regression:
            - A linear approach to modelling the relationship between a dependent variable and one or more independent variables.
        • R² (Coefficient of Determination):
            - A statistical measure of how well the regression predictions approximate the real data points.
        • MSE (Mean Squared Error):
            - The average squared difference between the estimated values and the actual value.

    Parameters:
        X_train (np.ndarray) : The transformed training feature set from LASSO feature selection.
        X_test  (np.ndarray) : The transformed test feature set from LASSO feature selection.
        y_train (pd.Series)  : The training target variable.
        y_test  (pd.Series)  : The test target variable.

    Produces:
        A scatter plot saved as a PNG file and displayed on the screen, showing the comparison between predicted and actual values.
    '''

    # 1: Fitting the Linear Regression model
    linear_regression = LinearRegression()
    linear_regression.fit(X_train, y_train)
    y_pred = linear_regression.predict(X_test)

    # 2: Plotting perfect prediction line
    plt.plot([y_test.min(), y_test.max()], [y_test.min(), y_test.max()], color = '#545B63', linestyle = '--')

    # 3: Plotting the scatter plot for actual vs predicted values
    plt.scatter(x = y_test, 
                y = y_pred, 
                c = y_test - y_pred,
                cmap = 'twilight_shifted',
                vmin = -1, 
                vmax = 1,
                edgecolor = None)

    # Displaying R² and MSE
    plt.text(0.5, 0.1, 
             (f"R²  ${r2_score(y_test, y_pred):.3f}$\n"
              f"MSE ${mean_squared_error(y_test, y_pred):.3f}$"), 
             fontsize = 12, fontweight = 'bold', linespacing = 1.8,
             bbox = dict(facecolor = '0.3', edgecolor = '0.3', boxstyle = 'round,pad = 0.75', alpha = 0.5),
             ha = 'left', va = 'center', transform = plt.gca().transAxes)

    plt.colorbar(label = 'Residuals')
    plt.xlabel('Total Cost')
    plt.ylabel('Predicted Values')
    plt.title('$08$: Linear Regression - Predictions vs. Actual Values')
    plt.tight_layout(pad = 2.0)

    # Saving the plot to a file
    file_path = find_project_root('./fig/analysis/jp/08 - Linear Regression Predictions vs Actual Values.png')
    plt.savefig(file_path)
    plt.show()

if __name__ == "__main__":
    
    df  = prepare_data()
    dfa = remove_anomalies(df)
    X_train, X_test, y_train, y_test, _ = lasso(dfa)
    slr(X_train, X_test, y_train, y_test)
