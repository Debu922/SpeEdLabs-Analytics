import numpy as np
def z_score(value, mean, std):
    if std == 0:
        return np.nan
    return (value - mean) / std