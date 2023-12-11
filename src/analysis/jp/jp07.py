import matplotlib.pyplot as plt
import numpy as np

from analysis.jp.jp06      import X_train_lasso
from sklearn.cluster       import KMeans
from sklearn.decomposition import PCA
from utils.runtime         import find_project_root

def kmeans(X: np.ndarray = X_train_lasso):
    '''
    Applies K-Means clustering and PCA (Principal Component Analysis) on the dataset for visualization.

    Methodology:
        1. Apply K-Means clustering to the dataset.
        2. Perform PCA for dimensionality reduction to 2 components.
        3. Visualize the clusters in the reduced dimensional space.

    Data Science Concepts:
        • K-Means Clustering:
            - An unsupervised machine learning algorithm used for clustering data into 'K' number of clusters.
            - Works by assigning data points to clusters such that the sum of the squared distance between the 
              data points and the cluster's centroid (arithmetic mean of all the data points that belong to that cluster) is minimized.
        • PCA (Principal Component Analysis):
            - A technique for reducing the dimensionality of datasets, increasing interpretability while minimizing information loss.
            - It transforms the original variables into a new set of variables (principal components) that are orthogonal 
              (uncorrelated), with the most important principal components carrying most of the variability in the data.

    Parameters:
        X (np.ndarray): The transformed training feature set from LASSO feature selection.

    Produces:
        A scatter plot saved as a PNG file and displayed on the screen, showing KMeans clusters in reduced dimensional space.
    '''

    # 1: Applying K-Means clustering
    kmeans   = KMeans(n_init = 'auto', random_state = 0)
    clusters = kmeans.fit_predict(X)

    # 2: Applying PCA for dimensionality reduction
    pca     = PCA(n_components = 2)
    reduced = pca.fit_transform(X.toarray())

    # 3: Visualizing the clusters in 2D
    plt.scatter(reduced[:, 0], 
                reduced[:, 1], 
                c    = clusters, 
                cmap = 'viridis')

    plt.xlabel('PCA Component 1')
    plt.ylabel('PCA Component 2')
    plt.title('$07$: KMeans Clusters in Reduced Dimensional Space ($PCA$)')
    plt.tight_layout(pad = 2.0)

    # Saving the plot to a file
    file_path = find_project_root('./fig/analysis/jp/07 - KMeans Clusters in Reduced Dimensional Space.png')
    plt.savefig(file_path)
    plt.show()
    

if __name__ == "__main__":
    
    kmeans()
