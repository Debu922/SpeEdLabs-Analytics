import numpy as np
def z_score(value, mean, std):
    if std == 0:
        return np.nan
    return (value - mean) / std

def weighted_avg_and_std(values, weights):
    """
    Return the weighted average and standard deviation.

    values, weights -- Numpy ndarrays with the same shape.
    """
    average = np.average(values, weights=weights)
    variance = np.average((values-average)**2, weights=weights)
    return np.array([average, np.sqrt(variance)])
