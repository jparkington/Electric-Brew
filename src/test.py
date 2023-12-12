import pandas as pd
from analysis.jp.flat import prepared_data
from sklearn.ensemble import IsolationForest

def print_anomaly_detection_stats(df: pd.DataFrame = prepared_data):
    '''
    Prints statistics for the data before and after applying anomaly detection.

    Outputs:
        - Basic statistics of 'total_cost' before and after anomaly detection.
        - Count of detected anomalies.
    '''

    # Fitting the Isolation Forest model
    isolation_forest = IsolationForest(contamination=0.001, n_jobs=-1, random_state=0)
    outliers = isolation_forest.fit_predict(df[['total_cost']])

    # Filtering the dataframe to remove detected anomalies
    df_without_anomalies = df[outliers == 1]

    # Print basic statistics before anomaly detection
    print("Before Anomaly Detection - 'total_cost' Statistics:")
    print(df['total_cost'].describe())

    # Print basic statistics after anomaly detection
    print("\nAfter Anomaly Detection - 'total_cost' Statistics:")
    print(df_without_anomalies['total_cost'].describe())

    # Count of detected anomalies
    anomaly_count = len(df) - len(df_without_anomalies)
    print("\nNumber of Detected Anomalies:", anomaly_count)

if __name__ == "__main__":
    print_anomaly_detection_stats()
