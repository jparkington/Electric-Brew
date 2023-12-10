import matplotlib.pyplot as plt

from analysis.jp.flat        import prepare_data
from analysis.jp.jp04        import remove_anomalies
from analysis.jp.jp06        import lasso
from sklearn.ensemble        import RandomForestRegressor
from sklearn.metrics         import mean_squared_error, r2_score
from sklearn.model_selection import train_test_split, RandomizedSearchCV
from utils.runtime           import find_project_root

def random_forest(X_train_lasso, y_train):
    '''
    Fits a Random Forest Regressor model using Randomized Search CV for hyperparameter tuning and visualizes predictions.

    Methodology:
        1. Perform a train/test split on the training dataset.
        2. Tune hyperparameters using Randomized Search CV.
        3. Fit the best Random Forest model and make predictions.
        4. Visualize the predicted vs. actual values and calculate R² and MSE.

    Data Science Concepts:
        • Random Forest:
            - An ensemble learning method that operates by constructing multiple decision trees during training.
            - For regression tasks, the output of the random forest is the mean prediction of the individual trees.
        • Hyperparameter Tuning:
            - The process of selecting the set of optimal hyperparameters for a learning algorithm, often using methods like 
              grid search or randomized search.

    Parameters:
        X_train_lasso (pd.DataFrame) : The transformed training feature set from LASSO feature selection.
        y_train       (pd.Series)    : The training target variable.

    Produces:
        A scatter plot saved as a PNG file and displayed on the screen, showing the comparison between predicted and actual values.
    '''

    # 1: Splitting the data
    X_train_rf, X_test_rf, y_train_rf, y_test_rf = train_test_split(X_train_lasso, y_train, random_state = 0)

    # 2: Initialize the Random Forest Regressor
    rf = RandomForestRegressor(random_state = 0)

    # Hyperparameter Grid
    distributions = {'n_estimators'      : [1, 2, 4, 8, 16],
                     'max_depth'         : [1, 2, 4, 8, 16],
                     'min_samples_split' : [2, 4, 8, 16],
                     'min_samples_leaf'  : [1, 2, 4, 8]}

    # 3: # Randomized Search with Cross-Validation
    random_search = RandomizedSearchCV(rf, distributions, n_jobs = -1, random_state = 0)
    random_search.fit(X_train_rf, y_train_rf)

    # Predictions using the best model
    best_rf   = random_search.best_estimator_
    y_pred_rf = best_rf.predict(X_test_rf)

    # 4: Visualizing predictions vs. actual values
    plt.plot([y_test_rf.min(), y_test_rf.max()], [y_test_rf.min(), y_test_rf.max()], color = '#545B63', linestyle = '--')
    plt.scatter(x = y_test_rf, 
                y = y_pred_rf, 
                c = y_test_rf - y_pred_rf,
                cmap = 'twilight_shifted',
                vmin = -1, 
                vmax = 1,
                edgecolor = None)

    # Text annotation for R² and MSE
    plt.text(0.5, 0.1, 
             (f"R²  ${r2_score(y_test_rf, y_pred_rf):.3f}$\n"
              f"MSE ${mean_squared_error(y_test_rf, y_pred_rf):.3f}$"), 
             fontsize = 12, fontweight = 'bold', linespacing = 1.8,
             bbox = dict(facecolor = '0.3', edgecolor = '0.3', boxstyle = 'round,pad = 0.75', alpha = 0.5),
             ha = 'left', va = 'center', transform = plt.gca().transAxes)

    # Second text annotation for hyperparameters (top left)
    plt.text(0.05, 0.95, 
             (f"Max Depth         ${best_rf.get_params()['max_depth']}$\n"
              f"Min Samples Split ${best_rf.get_params()['min_samples_split']}$\n"
              f"Min Samples Leaf  ${best_rf.get_params()['min_samples_leaf']}$\n"
              f"# of Estimators   ${best_rf.get_params()['n_estimators']}$"), 
             fontsize = 9, fontweight = 'bold', linespacing = 1.3,
             bbox = dict(facecolor = '0.3', edgecolor = '0.3', boxstyle = 'round,pad = 0.75', alpha = 0.5),
             ha = 'left', va = 'top', transform = plt.gca().transAxes)

    plt.colorbar(label = 'Residuals')
    plt.xlabel('Total Cost')
    plt.ylabel('Predicted Values')
    plt.title('$09$: Random Forest - Predictions vs. Actual Values')
    plt.tight_layout(pad = 2.0)

    # Saving the plot to a file
    file_path = find_project_root('./fig/analysis/jp/09 - Random Forest Predictions vs Actual Values.png')
    plt.savefig(file_path)
    plt.show()

if __name__ == "__main__":
    
    df  = prepare_data()
    dfa = remove_anomalies(df)
    X_train_lasso, _, y_train, _ = lasso(dfa)
    random_forest(X_train_lasso, y_train)
