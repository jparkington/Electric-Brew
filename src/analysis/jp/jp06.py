import matplotlib.pyplot as plt
import pandas  as pd
import seaborn as sns
import re

from analysis.jp.flat          import prepare_data
from analysis.jp.jp04          import remove_anomalies
from sklearn.compose           import ColumnTransformer
from sklearn.feature_selection import SelectFromModel
from sklearn.linear_model      import LassoCV
from sklearn.model_selection   import train_test_split
from sklearn.pipeline          import Pipeline
from sklearn.preprocessing     import OneHotEncoder, StandardScaler
from utils.runtime             import find_project_root

def lasso(df: pd.DataFrame):
    '''
    Applies LASSO feature selection to determine important features for predicting 'total_cost'.

    Methodology:
        1. Preprocess the data by dropping irrelevant columns and handling categorical and numerical features.
        2. Split the data into training and test sets.
        3. Fit a LASSO model to the training data.
        4. Visualize the feature importance determined by LASSO.

    Data Science Concepts:
        • LASSO (Least Absolute Shrinkage and Selection Operator):
            - A regression analysis method that performs both variable selection and regularization to enhance the 
              prediction accuracy and interpretability of the resulting statistical model.

    Parameters:
        df (pd.DataFrame): The dataframe returned from the `remove_anomalies` function.

    Produces:
        A bar plot saved as a PNG file and displayed on the screen, showing the importance of each feature determined by LASSO.
    '''

    # 1: Preprocessing the data
    dff = df.loc[:, ~df.columns.str.contains('id')] \
            .drop(['billing_interval', 'invoice_number', 'street', 'label', 'source',
                   'account_number', 'account_number_dm', 'account_number_db',
                   'kwh', 'period', 'week', 'month', 'quarter', 
                   'delivery_cost', 'supply_cost', 'tax_cost', 'service_charge'], axis = 1) \
            .dropna()

    X = dff.drop(['total_cost'], axis=1)
    y = dff['total_cost']

    # Define features to engineer
    categorical_features = X.select_dtypes(include = ['object', 'category']).columns.union(['hour'])
    numeric_features     = X.select_dtypes(include = ['int64', 'float64']).columns

    # 2: Creating a pipeline with preprocessing and LASSO
    model = Pipeline(steps = [('preprocessor',     ColumnTransformer([('num', StandardScaler(), numeric_features),
                                                                      ('cat', OneHotEncoder(),  categorical_features)])),

                              ('feature_selector', SelectFromModel(LassoCV(max_iter     = 50000, 
                                                                           n_jobs       = -1, 
                                                                           random_state = 0)))])

    # 3: Splitting the data, fitting the model, and selecting features
    X_train, X_test, y_train, y_test = train_test_split(X, y, random_state=0)
    model.fit(X_train, y_train)

    # Accessing the fitted LassoCV model, getting its feature names, and applying the SelectFromModel mask
    lasso_cv        = model.named_steps['feature_selector'].estimator_
    feature_names   = model.named_steps['preprocessor'].get_feature_names_out()
    selection_mask  = model.named_steps['feature_selector'].get_support()
    shortened_names = [re.split('[: ]', name)[0] for name in feature_names[selection_mask]]

    # Creating a Series for easy plotting
    feature_importance = pd.Series(data  = lasso_cv.coef_[selection_mask], 
                                   index = shortened_names).sort_values(ascending = False)

    # 4: Barplot visualization
    sns.barplot(x       = feature_importance, 
                y       = feature_importance.index, 
                hue     = feature_importance, 
                legend  = False,
                palette = 'twilight')

    plt.xlabel('Coefficient Value')
    plt.ylabel('Feature')
    plt.title('$06$: LASSO Feature Selection for Determining Total Cost')
    plt.tight_layout(pad = 2.0)

    # Saving the plot to a file
    file_path = find_project_root('./fig/analysis/jp/06 - Feature Selection for Determining Total Cost.png')
    plt.savefig(file_path)
    plt.show()

if __name__ == "__main__":
    
    df  = prepare_data()
    dfa = remove_anomalies(df)
    lasso(dfa)
