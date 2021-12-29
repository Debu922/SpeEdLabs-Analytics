import pandas as pd
import numpy as np
from util import *

class Topic():
    def __init__(self, topicId) -> None:
        self.topicId = topicId
        self.questionIds = pd.Series()
        self.metrics = pd.DataFrame()

        return 

    def gen_metrics(self, sessionData):
        sD = sessionData[sessionData["TopicId"] == self.topicId]
        columns = ["NoQ", "Att", "Acc", "STim", "ATim", "KSC","Soln", "Att2", "Acc2", "Revw"]       
        index = ["Avg", "Std"]
        tM = pd.DataFrame(columns=columns, index = index)
        tM.loc["Avg", "NoS" ], tM.loc["Std", "NoS" ] = (sD.shape[0],0)
        tM.loc["Avg", "NoQ" ], tM.loc["Std", "NoQ" ] = (np.mean(sD["_TotalQuestions"]), np.std(sD["_TotalQuestions"]))
        tM.loc["Avg", "Att" ], tM.loc["Std", "Att" ] = weighted_avg_and_std(100.0*np.divide(sD["_TotalAttempted"],sD["_TotalQuestions"]), sD["_TotalQuestions"])
        tM.loc["Avg", "Acc" ], tM.loc["Std", "Acc" ] = weighted_avg_and_std(100.0*np.divide(sD["_TotalCorrect"],sD["_TotalQuestions"]), sD["_TotalQuestions"])
        tM.loc["Avg", "STim"], tM.loc["Std", "STim"] = (np.mean(sD["_TotalTimeTaken"]) , np.std(sD["_TotalTimeTaken"]))
        self.metrics = tM

        return