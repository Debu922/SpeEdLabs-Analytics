
import pandas as pd
import numpy as np
from pandas._libs import indexing

from util import weighted_avg_and_std

class Subject():
    def __init__(self, id, type:str, subjectId:int, cnxn) -> None:
        self.id = id
        self.type = type
        self.subjectId = subjectId

        self.chapterIds = pd.Series()
        self.metrics = None
        self.sessionMetricsData = None

        self.sessions = pd.Series()

        self.cnxn = cnxn

        return
    def get_subject_metrics(self,subjectData):
        if type(self.metrics) == pd.DataFrame:
            return
        if self.type == "user":
            self.get_user_subject_metrics()
        else:
            self.get_global_subject_metrics(subjectData)
        return

    def get_user_subject_metrics(self):
        if type(self.sessionMetricsData) == pd.DataFrame:
            return
        columns = ["NoQ", "Att", "Acc", "STim", "ATim", "KSC","Soln", "Att2", "Acc2", "Revw"]
        self.sessionMetricsData = pd.DataFrame(columns=columns)
        for session in self.sessions:
            session.gen_metrics(self.cnxn)
            # print(session.metrics)
            
            self.sessionMetricsData.loc[session.userSessionId] = session.metrics
            # self.sessionMetricsData[session.userSessionId] = session.metrics
        
        sM = pd.DataFrame()

        sD = self.sessionMetricsData
        # print(sD)
        sM.loc["Avg", "NoS" ], sM.loc["Std", "NoS" ] = (sD.shape[0],0)
        sM.loc["Avg", "NoQ" ], sM.loc["Std", "NoQ" ] = (np.mean(sD["NoQ"]), np.std(sD["NoQ"]))
        sM.loc["Avg", "Att" ], sM.loc["Std", "Att" ] = weighted_avg_and_std(sD["Att"],sD["NoQ"])
        sM.loc["Avg", "Acc" ], sM.loc["Std", "Acc" ] = weighted_avg_and_std(sD["Acc"] ,sD["NoQ"])
        sM.loc["Avg", "ATim"], sM.loc["Std", "ATim"] = weighted_avg_and_std(sD["ATim"],sD["NoQ"])
        sM.loc["Avg", "STim"], sM.loc["Std", "STim"] = (np.mean(sD["STim"]) , np.std(sD["STim"]))
        sM.loc["Avg", "KSC" ], sM.loc["Std", "KSC" ] = weighted_avg_and_std(sD["KSC" ],sD["NoQ"])
        sM.loc["Avg", "Soln"], sM.loc["Std", "Soln"] = weighted_avg_and_std(sD["Soln"],sD["NoQ"])
        sM.loc["Avg", "Att2"], sM.loc["Std", "Att2"] = weighted_avg_and_std(sD["Att2"],sD["NoQ"])
        sM.loc["Avg", "Acc2"], sM.loc["Std", "Acc2"] = weighted_avg_and_std(sD["Acc2"],sD["NoQ"])
        sM.loc["Avg", "Revw"], sM.loc["Std", "Revw"] = weighted_avg_and_std(sD["Revw"],sD["NoQ"])
        self.metrics = sM
        return

    def get_global_subject_metrics(self,subjectData):
        if type(self.sessionMetricsData) == pd.DataFrame:
            return
        session = self.sessions.iloc[0]
        session.gen_metrics(self.cnxn)
        self.sessionMetricsData = pd.DataFrame(columns=session.metrics.index)
        for session in self.sessions:
            session.gen_metrics(self.cnxn)
            self.sessionMetricsData.loc[session.userSessionId] = session.metrics
            # self.sessionMetricsData[session.userSessionId] = session.metrics
        
        sM = pd.DataFrame()
        
        sD = self.sessionMetricsData
        sM.loc["Avg", "Total Sessions"] = sD.shape[0]
        sM.loc["Avg", "NoS" ], sM.loc["Std", "NoS" ] = (np.mean(subjectData.groupby("UserId")['UserId'].nunique()), 0.0)
        sM.loc["Avg", "NoQ" ], sM.loc["Std", "NoQ" ] = (np.mean(sD["NoQ"]), np.std(sD["NoQ"]))
        sM.loc["Avg", "Att" ], sM.loc["Std", "Att" ] = weighted_avg_and_std(sD["Att"],sD["NoQ"])
        sM.loc["Avg", "Acc" ], sM.loc["Std", "Acc" ] = weighted_avg_and_std(sD["Acc"] ,sD["NoQ"])
        sM.loc["Avg", "ATim"], sM.loc["Std", "ATim"] = weighted_avg_and_std(sD["ATim"],sD["NoQ"])
        sM.loc["Avg", "STim"], sM.loc["Std", "STim"] = (np.mean(sD["STim"]) , np.std(sD["STim"]))
        sM.loc["Avg", "KSC" ], sM.loc["Std", "KSC" ] = weighted_avg_and_std(sD["KSC" ],sD["NoQ"])
        sM.loc["Avg", "Soln"], sM.loc["Std", "Soln"] = weighted_avg_and_std(sD["Soln"],sD["NoQ"])
        sM.loc["Avg", "Att2"], sM.loc["Std", "Att2"] = weighted_avg_and_std(sD["Att2"],sD["NoQ"])
        sM.loc["Avg", "Acc2"], sM.loc["Std", "Acc2"] = weighted_avg_and_std(sD["Acc2"],sD["NoQ"])
        sM.loc["Avg", "Revw"], sM.loc["Std", "Revw"] = weighted_avg_and_std(sD["Revw"],sD["NoQ"])
        self.metrics = sM
        return

