import pandas as pd
import numpy as np

from db_util import weighted_avg_and_std

class Session():

    def __init__(self, student, type: str, userSessionId: int, config) -> None:
        self.sessionType = type
        self.student = student
        self.userSessionId = userSessionId
        self.questionData = None
        self.metrics = None
        self.config = config

        pass

    def __str__(self) -> str:
        # TODO Fix this and make it more presentable
        output = \
            """Session details:
        Session type =      {}
        UserSessionId =     {}""".format(self.sessionType, self.userSessionId)
        return output

    def fetch_question_data(self, cnxn):
        if type(self.questionData) == pd.DataFrame:
            return
        if self.sessionType == "user":
            self.fetch_user_question_data(cnxn)

        if self.sessionType == "pseudo":
            self.fetch_pseudo_question_data(cnxn)
 
    def fetch_user_question_data(self, cnxn):
        query = """
        SELECT 
            [QuestionId]
            ,[IsCorrectlyAnswered]
            ,[TimeTakenTillSubmission]
            ,[IsKSCSeen]
            ,[IsFullSolutionSeen]
            ,[IsCorrectlyAnsweredSecondTime]
            ,[MarkedForRevision]
        FROM 
            [speedlabs-anon].[dbo].[UserTestSessionQuestion]
        WHERE
            [userTestSessionId] = {x}
        """.format(x=self.userSessionId)
        questionData = pd.read_sql(query, cnxn).set_index("QuestionId")

        # Rename columns
        questionData.rename(columns={"QuestionId": "QId",
                                     "IsCorrectlyAnswered": "Acc",
                                     "TimeTakenTillSubmission": "Time",
                                     "IsKSCSeen": "KSC",
                                     "IsFullSolutionSeen": "Soln",
                                     "IsCorrectlyAnsweredSecondTime": "Acc2",
                                     "MarkedForRevision": "Revw"}, inplace=True)

        # Generate attempt data
        Att = questionData["Acc"].notnull()
        Att2 = questionData["Acc2"].notnull()
        questionData.insert(loc=1, column="Att", value=Att)
        questionData.insert(loc=5, column="Att2", value=Att2)

        self.questionData = questionData
        return

    def fetch_pseudo_question_data(self, cnxn):
        query = """
        SELECT 
            [QuestionId]
            ,[IsCorrectlyAnswered]
            ,[TimeTakenTillSubmission]
            ,[IsKSCSeen]
            ,[IsFullSolutionSeen]
            ,[IsCorrectlyAnsweredSecondTime]
            ,[MarkedForRevision]
        FROM 
            [speedlabs-anon].[dbo].[UserTestSessionQuestion]
        WHERE
            [QuestionId] in (
                SELECT
                    [QuestionId]
                FROM 
                    [speedlabs-anon].[dbo].[UserTestSessionQuestion]
                WHERE
                    [UserTestSessionId] = {x}
            )
        """.format(x=self.userSessionId)
        questionData = pd.read_sql(query, cnxn)
        questionData.rename(columns={"QuestionId": "QId",
                                     "IsCorrectlyAnswered": "Acc",
                                     "TimeTakenTillSubmission": "Time",
                                     "IsKSCSeen": "KSC",
                                     "IsFullSolutionSeen": "Soln",
                                     "IsCorrectlyAnsweredSecondTime": "Acc2",
                                     "MarkedForRevision": "Revw"}, inplace=True)
        columns = ["QId", "App", "Att", "Acc", "Time", "StdT",
                   "KSC", "Soln", "Att2", "Acc2", "Revw"]
        df = pd.DataFrame(columns=columns).set_index("QId")
        qIds = np.unique(questionData["QId"])
        for qId in qIds:
            qData = questionData[questionData["QId"] == qId]

            df.loc[qId,"App" ] = qData.shape[0]
            df.loc[qId,"Att" ] = 100.0 * np.sum(qData["Acc"].notnull()) / qData.shape[0]
            df.loc[qId,"Acc" ] = 100.0 * np.sum(qData["Acc"]) / qData.shape[0]
            df.loc[qId,"Time"] = np.mean(qData["Time"])
            df.loc[qId,"StdT"] = np.std(qData["Time"])
            df.loc[qId,"KSC" ] = 100.0 * np.sum(qData["KSC"]) / qData.shape[0]
            df.loc[qId,"Soln"] = 100.0 * np.sum(qData["Soln"]) / qData.shape[0]
            df.loc[qId,"Att2"] = 100.0 * np.sum(qData["Acc2"].notnull()) / (df.loc[qId,"App"] - np.sum(qData["Acc"]))
            df.loc[qId,"Acc2"] = 100.0 * np.sum(qData["Acc2"]) / (df.loc[qId,"App"] - np.sum(qData["Acc"]))
            df.loc[qId,"Revw"] = 100.0 * np.sum(qData["Revw"]) / qData.shape[0]
        
        self.questionData = df

    def gen_metrics(self,cnxn):
    
        self.fetch_question_data(cnxn)
        if type(self.metrics) == pd.DataFrame:
            return
        if self.sessionType == "user":
            self.gen_user_metrics()
        if self.sessionType == "pseudo":
            self.gen_pseudo_metrics()
        return

    def gen_user_metrics(self):
               
        # User Metrics
        uM = pd.Series()
        # Question Data
        qD = self.questionData
        uM.loc["NoQ"] = qD.shape[0]
        uM.loc["Att"] = 100.0* np.sum(qD["Att"]) / qD.shape[0]
        uM.loc["Acc"] = 100.0* np.sum(qD["Acc"]) / qD.shape[0]
        uM.loc["TTim"] = np.sum(qD["Time"])
        uM.loc["ATim"] = np.mean(qD["Time"])
        uM.loc["KSC"] = 100.0* np.sum(qD["KSC"]) / qD.shape[0]
        uM.loc["Soln"] = 100.0* np.sum(qD["Soln"]) / qD.shape[0]
        uM.loc["Att2"] = 100.0* np.sum(qD["Att2"]) / (qD.shape[0] - np.sum(qD["Acc"]))
        uM.loc["Acc2"] = 100.0* np.sum(qD["Acc2"]) / (qD.shape[0] - np.sum(qD["Acc"]))
        uM.loc["Revw"] = 100.0* np.sum(qD["Revw"]) / qD.shape[0]

        self.metrics = uM#.style.set_caption("User Session Metrics")
        return

    def weighted_avg_and_std(values, weights):
        average = np.average(values, weights=weights)
        # Fast and numerically precise:
        variance = np.average((values-average)**2, weights=weights)
        return (average, np.sqrt(variance))

    def gen_pseudo_metrics(self):
        columns = ["NoQ", "Att", "Acc", "TTim", "ATim", "KSC","Soln", "Att2", "Acc2", "Revw"]
        index = ["Avg","Std"]
        
        pM = pd.DataFrame(columns =columns ,index = index)
        qD = self.questionData

        pM.loc[ "Avg","NoQ" ], pM.loc[ "Std","NoQ" ] = qD.shape[0], 0
        pM.loc[ "Avg","Att" ], pM.loc[ "Std","Att" ] = weighted_avg_and_std(qD["Att"], qD["App"])
        pM.loc[ "Avg","Acc" ], pM.loc[ "Std","Acc" ] = weighted_avg_and_std(qD["Acc"], qD["App"])
        pM.loc[ "Avg","KSC" ], pM.loc[ "Std","KSC" ] = weighted_avg_and_std(qD["KSC"], qD["App"])
        pM.loc[ "Avg","Soln"], pM.loc[ "Std","Soln"] = weighted_avg_and_std(qD["Soln"], qD["App"])
        pM.loc[ "Avg","Att2"], pM.loc[ "Std","Att2"] = weighted_avg_and_std(qD["Att2"], qD["App"])
        pM.loc[ "Avg","Acc2"], pM.loc[ "Std","Acc2"] = weighted_avg_and_std(qD["Acc2"], qD["App"])
        pM.loc[ "Avg","Revw"], pM.loc[ "Std","Revw"] = weighted_avg_and_std(qD["Revw"], qD["App"])
        pM.loc[ "Avg","ATim"], pM.loc[ "Std","ATim"] = weighted_avg_and_std(qD["Time"], qD["App"])
        pM.loc[ "Avg","TTim"] = np.sum(qD["Time"])
        pM.loc[ "Std","TTim"] = np.sqrt(np.sum(qD["StdT"]**2))
        self.metrics = pM
         
        return
