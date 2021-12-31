from typing import Dict, Text
import pandas as pd
import json
import sys
from db_util import get_session_metrics
from institute import Institute
from session import Session
from user import User
from util import *
import json
import os

class Feedback():
    def __init__(self, type, id, data) -> None:
        self.type = type
        self.id = id
        self.data = data
        return

class FeedbackEngine():

    def __init__(self, cnxn, config) -> None:
        """FeedbackEngine class that deals with generating feedback and controlling users and instiutes.

        Args:
            cnxn (Connection): Connection to database
            config (Dict): Config file
        """

        # Initate users and institute lists
        self.users = pd.Series()
        self.institutes = pd.Series()
        self.sessions = pd.Series()
        self.zScoresSQR = pd.Series(dtype=object)
        self.feedbackSQRList = pd.Series()

        # DB Connection
        self.cnxn = cnxn

        # Configs
        self.config = config
        with open(os.getcwd()+"/../practice_session_config.json") as file:
            self.PSConfig = json.load(file)

        # Debug line
        if self.config["debug"]:
            print("FeedbackEngine initialization complete")

    def gen_session_quality_feedback_list(self, sessionId, historyLength):
        """Generate a session quality report from sessionId and session history length.

        Args:
            sessionId (int): sessionId for which report has to be generated.
            historyLength (int): How many sessions to consider for the session history.

        Returns:
            zScores, [type]: [description]
        """
        
        # Get Z Scores of sessions for session quality report.
        zScores, uM, pM, sM, uHM = self.get_SQualR_ZScores(sessionId, historyLength)

        # Setup config file.
        sqrConfig:Dict
        sqrConfig = self.PSConfig["sessionQuality"]

        # Get metrics to be evaluated from config.
        metricEvaluated = sqrConfig.keys()

        # Make dataframe to store feedback report
        feedbackListColums = ["metric", "type", "id", "score", "text"]
        feedbackList = pd.DataFrame(columns=feedbackListColums)

        # Get Metric data to put into text.
        metric_data = {
            "_userNoQ"  : uM.loc["NoQ" ],
            "_userAtt"  : uM.loc["Att" ],
            "_userAcc"  : uM.loc["Acc" ],
            "_userKSC"  : uM.loc["KSC" ],
            "_userSoln" : uM.loc["Soln"],
            "_userAtt2" : uM.loc["Att2"],
            "_userAcc2" : uM.loc["Acc2"],
            "_userRevw" : uM.loc["Revw"],
            "_userATim" : uM.loc["ATim"],
            "_userSTim" : uM.loc["STim"],

            "_pseudoNoQ"  : pM.loc["Avg", "NoQ" ],
            "_pseudoAtt"  : pM.loc["Avg", "Att" ],
            "_pseudoAcc"  : pM.loc["Avg", "Acc" ],
            "_pseudoKSC"  : pM.loc["Avg", "KSC" ],
            "_pseudoSoln" : pM.loc["Avg", "Soln"],
            "_pseudoAtt2" : pM.loc["Avg", "Att2"],
            "_pseudoAcc2" : pM.loc["Avg", "Acc2"],
            "_pseudoRevw" : pM.loc["Avg", "Revw"],
            "_pseudoATim" : pM.loc["Avg", "ATim"],
            "_pseudoSTim" : pM.loc["Avg", "STim"],

            "_subjectNoQ"  : sM.loc["Avg", "NoQ" ],
            "_subjectAtt"  : sM.loc["Avg", "Att" ],
            "_subjectAcc"  : sM.loc["Avg", "Acc" ],
            "_subjectKSC"  : sM.loc["Avg", "KSC" ],
            "_subjectSoln" : sM.loc["Avg", "Soln"],
            "_subjectAtt2" : sM.loc["Avg", "Att2"],
            "_subjectAcc2" : sM.loc["Avg", "Acc2"],
            "_subjectRevw" : sM.loc["Avg", "Revw"],
            "_subjectATim" : sM.loc["Avg", "ATim"],
            "_subjectSTim" : sM.loc["Avg", "STim"],

            "_historyNoQ"  : uHM.loc["Avg", "NoQ" ],
            "_historyAtt"  : uHM.loc["Avg", "Att" ],
            "_historyAcc"  : uHM.loc["Avg", "Acc" ],
            "_historyKSC"  : uHM.loc["Avg", "KSC" ],
            "_historySoln" : uHM.loc["Avg", "Soln"],
            "_historyAtt2" : uHM.loc["Avg", "Att2"],
            "_historyAcc2" : uHM.loc["Avg", "Acc2"],
            "_historyRevw" : uHM.loc["Avg", "Revw"],
            "_historyATim" : uHM.loc["Avg", "ATim"],
            "_historySTim" : uHM.loc["Avg", "STim"]
        }

        # Iterate through Z-Scores and 
        for metric in metricEvaluated:
            for tableName in sqrConfig[metric].keys():
                # Get ZScore to be compared
                zScore = zScores.loc[tableName,metric]

                # Iterate through option ids.
                for id in sqrConfig[metric][tableName].keys(): 

                    # Compare if within bound and update FeedbackList
                    if sqrConfig[metric][tableName][id]["z-range"][0] < zScore and zScore <= sqrConfig[metric][tableName][id]["z-range"][1]:
                        df = pd.DataFrame({"metric":[metric],
                                           "type":[tableName],
                                           "id":[id],
                                           "score":[zScore*sqrConfig[metric][tableName][id]["modifier"]],
                                           "text":[sqrConfig[metric][tableName][id]["text"].format(**metric_data)]})
                        feedbackList = feedbackList.append(df, ignore_index=True)
                        break
        if self.config["debug"]:
            print("Feedback List Generated successfully.")
        return feedbackList
        
    def get_SQualR_ZScores(self, sessionId, historyLength):
        """Generate Session Quality Report Zscores

        Args:
            sessionId (int): Session ID for which quality report is to be generated
            historyLength (int): How long should session history be

        Returns:
            sessionZScore: Zscore for session
        """
        # Generate User Class
        userId = self.get_userId_from_sessionId(sessionId)
        if userId in self.users.index:
            user = self.users[userId]
            if self.config["debug"]:
                print("User Class Already exists, fetching existing user data")
        else:
            user = self.users.loc[userId] = User(userId, self.cnxn, self.config)
            if self.config["debug"]:
                print("User Class Generated")

        # Generate Institute Class
        instiuteId = 0
        if instiuteId in self.institutes.index:
            instiute = self.institutes[instiuteId]
            if self.config["debug"]:
                print("Institute Class already exists, fetching existing institute data")
        else:
            instiute = self.institutes.loc[instiuteId] = Institute(instiuteId, self.cnxn, self.config)
            if self.config["debug"]:
                print("Institute Class Generated")

        # Generate User Session Metrics
        user.gen_session_metrics(sessionId)
        if self.config["debug"]:
            print("User Session Metrics Generated")
        
        # Generate Institute Pseudo Session Metrics
        instiute.gen_session_metrics(sessionId)
        if self.config["debug"]:
            print("Pseudo Session Metrics Generated")


        # Generatue User Subject Metrics
        subjectId = int(user.sessionData.loc[sessionId, "SubjectId"])
        user.gen_subject_metrics(subjectId)
        if self.config["debug"]:
            print("User Subject Metrics Generated")


        # Generate User Session History Metrics
        user.gen_user_session_history(sessionId, historyLength)
        if self.config["debug"]:
            print("User Session History Metrics Generated")


        # Begin Generating metrics
        flags = ["NoQ", "Att", "Acc", "STim", "ATim", "KSC","Soln", "Att2", "Acc2", "Revw"]

        sessionZScore = pd.DataFrame(columns = flags)
        
        # Session vs Pseudo        
        uM = user.sessions[sessionId].metrics
        pM = instiute.sessions[sessionId].metrics
        for flag in flags:
            sessionZScore.loc[("Session vs Pseudo Session",flag)] = z_score(uM.loc[flag],pM.loc["Avg",flag],pM.loc["Std",flag])

        # Session vs User Subject History
        sM = user.subjects[subjectId].metrics
        for flag in flags:
            sessionZScore.loc[("Session vs Subject History",flag)] = z_score(uM.loc[flag],sM.loc["Avg",flag],sM.loc["Std",flag])
        # Session vs User Session History
        uHM = user.sessionHistoryMetrics[sessionId]
        for flag in flags:
            sessionZScore.loc[("Session vs Session History",flag)] = z_score(uM.loc[flag],uHM.loc["Avg",flag],uHM.loc["Std",flag])
        if self.config["debug"]:
            print("SessionZScore calculated")      
        return sessionZScore, uM, pM, sM, uHM

    def get_session_quantity_feedback_list(self, sessionId):
        zScores, uM, tM = self.get_SQuanR_ZScores(sessionId)
        sqrConfig:Dict
        sqrConfig = self.PSConfig["sessionQuantity"]

        # Get metrics to be evaluated from config.
        metricEvaluated = sqrConfig.keys()

        # Make dataframe to store feedback report
        feedbackListColums = ["metric", "type", "id", "score", "text"]
        feedbackList = pd.DataFrame(columns=feedbackListColums)

        metric_data = {
            "_userNoQ"  : uM.loc["NoQ" ],
            "_userAtt"  : uM.loc["Att" ],
            "_userAcc"  : uM.loc["Acc" ],
            "_userKSC"  : uM.loc["KSC" ],
            "_userSoln" : uM.loc["Soln"],
            "_userAtt2" : uM.loc["Att2"],
            "_userAcc2" : uM.loc["Acc2"],
            "_userRevw" : uM.loc["Revw"],
            "_userATim" : uM.loc["ATim"],
            "_userSTim" : uM.loc["STim"],

            "_topicNoS" : tM.loc["Avg","NoS" ],
            "_topicNoQ" : tM.loc["Avg","NoQ" ],
            "_topicAtt" : tM.loc["Avg","Att" ],
            "_topicAcc" : tM.loc["Avg","Acc" ],
            "_topicSTim": tM.loc["Avg","STim"]
        }

        # Iterate through Z-Scores and 
        for metric in metricEvaluated:
            for tableName in sqrConfig[metric].keys():
                # Get ZScore to be compared
                zScore = zScores.loc[tableName,metric]

                # Iterate through option ids.
                for id in sqrConfig[metric][tableName].keys(): 

                    # Compare if within bound and update FeedbackList
                    if sqrConfig[metric][tableName][id]["z-range"][0] < zScore and zScore <= sqrConfig[metric][tableName][id]["z-range"][1]:
                        df = pd.DataFrame({"metric":[metric],
                                           "type":[tableName],
                                           "id":[id],
                                           "score":[zScore*sqrConfig[metric][tableName][id]["modifier"]],
                                           "text":[sqrConfig[metric][tableName][id]["text"].format(**metric_data)]})
                        feedbackList = feedbackList.append(df, ignore_index=True)
                        break
        if self.config["debug"]:
            print("Feedback List Generated successfully.")
        return feedbackList

    def get_SQuanR_ZScores(self, sessionId):
        """Generate Session Quantity Report Zscores

        Args:
            sessionId (int): Session ID for which quality report is to be generated
        Returns:
            sessionZScore: Zscore for session
        """
        # Generate User Class
        userId = self.get_userId_from_sessionId(sessionId)
        if userId in self.users.index:
            user = self.users[userId]
            if self.config["debug"]:
                print("User Class Already exists, fetching existing user data")
        else:
            user = self.users.loc[userId] = User(userId, self.cnxn, self.config)
            if self.config["debug"]:
                print("User Class Generated")

        # Generate Institute Class
        instiuteId = 0
        if instiuteId in self.institutes.index:
            instiute = self.institutes[instiuteId]
            if self.config["debug"]:
                print("Institute Class already exists, fetching existing institute data")
        else:
            instiute = self.institutes.loc[instiuteId] = Institute(instiuteId, self.cnxn, self.config)
            if self.config["debug"]:
                print("Institute Class Generated")

        # Generate User Session Metrics
        user.gen_session_metrics(sessionId)
        topicId = int(user.sessionData.loc[sessionId, "TopicId"])
        if self.config["debug"]:
            print("User Session Metrics Generated")
        
        # Generate Instiute Topic Metrics:
        instiute.get_topic_metrics(topicId)
        if self.config["debug"]:
            print("Institute Topic Metrics Generated")


        # Begin Generating metrics
        flags = ["NoQ", "Att", "Acc", "STim", "ATim", "KSC","Soln", "Att2", "Acc2", "Revw"]

        sessionZScore = pd.DataFrame(columns = flags)
        
        # Session vs Topic        
        uM = user.sessions[sessionId].metrics
        tM = instiute.topic[topicId].metrics
        for flag in flags:
            sessionZScore.loc[("Session vs Global Topic",flag)] = z_score(uM.loc[flag],tM.loc["Avg",flag],tM.loc["Std",flag]) 
        return sessionZScore, uM, tM

    def gen_session_quantity_report(self, sessionId):
        userId = self.get_userId_from_sessionId(sessionId)
        if userId in self.users.index:
            user = self.users.loc[userId]
            if self.config["debug"]:
                print("User Class Exists, fetching existing user.")
        else:
            user = self.users.loc[userId] = User(userId, self.cnxn, self.config)
            if self.config["debug"]:
                print("User Class Generated")

        
        
        return

    def gen_session_coverage_report(self,):
        return

    def get_userId_from_sessionId(self, sessionId):
        """Get the userId from sessionId by querying the SQL server.        
        """
        query = """
            SELECT
                [UserId]
            FROM
                [speedlabs-anon].[dbo].[UserTestSession]
            WHERE
                [UserTestSessionId] = {}
        """.format(str(sessionId))
        data = pd.read_sql(query, self.cnxn)
        return int(data.iloc[0])