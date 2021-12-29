from chapter import Chapter
from session import Session
from subject import Subject
import pandas as pd
from db_util import DBConnection
import numpy as np
import sys

from util import *

class User():
    def __init__(self, userId, cnxn, config, instituteId = 0):
        """Basic user class that holds details about the users and his sessions.

        Args:
            userId (int): userId
            cnxn (Connection): SQL connection to the DB
            config (Dict): Config file for user
            instituteId (int): Institute Id (Default = 0)
        """

        # Basic Data
        self.userId = userId
        self.instituteId = userId

        # Session Metrics
        self.sessionData = pd.DataFrame()
        self.sessions = pd.Series(dtype=object)
        
        # Chapter Metrics
        self.chapterData = pd.DataFrame()
        self.chapters = pd.Series(dtype=  object)

        # Subject Metrics
        self.subjectData = pd.DataFrame()
        self.subjects = pd.Series(dtype = object)

        # SQL Connection
        self.cnxn = cnxn

        # Config
        self.config = config
        
        # Feedback reports
        ind = pd.MultiIndex.from_arrays([[]] * 2, names=(u'SessionId', u'Type'))
        self.ZScores = pd.DataFrame(dtype = object, index=ind)
        self.userId = userId
        self.ZScoresType = [
            "userSession vs pseudoSession",
            "userSession vs userSubject",
            "userSession vs globalSubject"
        ]
        return

    def get_sessions(self, top = 20, sessionIds = None) -> None:
        """Fetch latest session data from database

        Args:
            top (int, optional): [description]. Defaults to 20.
        """        
        # sessionFromIDs
        if type(sessionIds) == pd.DataFrame:
            data = self.get_sessions_from_IDs(sessionIds)
        else:
            data = self.get_top_sessions(top)

        # Combine data with preexisting data
        self.sessionData = self.sessionData.combine_first(data)
        # Generate session objects
        for sessionIds in self.sessionData.index:
            if sessionIds in self.sessions.index:
                continue
            self.sessions.loc[sessionIds] = Session(self,"user",sessionIds, None)

        return
    
    def get_sessions_from_IDs(self, sessionIds):
        if type (sessionIds) == int:
            sessionIds = [sessionIds]
        query = """
        SELECT
            [UserTestSessionId]
            ,[CourseId]
            ,[SubjectId]
            ,[TopicId]
            ,[EndedOn]
            ,[_TotalQuestions]
            ,[_TotalAttempted]
            ,[_TotalCorrect]
            ,[_TotalTimeTaken]
        FROM 
            [speedlabs-anon].[dbo].[UserTestSession]
        WHERE
            [UserTestSessionId] {}
        """
        if len(sessionIds)==1:
            query = query.format("= "+ str(sessionIds[0]))
        else:
            query = query.format("IN (" +", ".join([str(x) for x in sessionIds]) + " )")
    
        return pd.read_sql(query,self.cnxn).set_index("UserTestSessionId")

    def get_top_sessions(self, top):
        query = """
        SELECT TOP ({_top})
            [UserTestSessionId]
            ,[CourseId]
            ,[SubjectId]
            ,[TopicId]
            ,[EndedOn]
            ,[_TotalQuestions]
            ,[_TotalAttempted]
            ,[_TotalCorrect]
            ,[_TotalTimeTaken]
        FROM 
            [speedlabs-anon].[dbo].[UserTestSession]
        WHERE 
            [UserId] = {_userId} AND
            [_TotalQuestions] >= {_minQuestions} AND
            [_TotalTimeTaken] >= {_minTime}
        ORDER BY
            [EndedOn] DESC
        """.format(_top=top, _userId = self.userId, _minQuestions = self.config["session"]["minQuestions"], _minTime = self.config["session"]["minTime"])
        return pd.read_sql(query,self.cnxn).set_index("UserTestSessionId")

    def gen_pseudo_session(self, sessionId):
        if sessionId in self.pseudoSessions.index:
            return
        self.pseudoSessions.loc[sessionId] = Session(self,"pseudo",sessionId, None)
    
    def gen_session_metrics(self, sessionIds):

        # Check type of sessionIds
        if type(sessionIds) == int:
            sessionIds = [sessionIds]
        
        # Fetch sessions if not already fetched
        self.get_sessions(sessionIds = sessionIds)

        # Generate metrics for each session
        for sessionId in sessionIds:
            self.sessions.loc[sessionId].gen_metrics(self.cnxn)
        return
    
    def get_chapters(self, chapterIds) -> None:


        return

    def gen_chapter_metrics(self) -> None:
        return

    def get_subjects(self, subjectIds) -> None:
        # Check type of chapterId
        if type(subjectIds) == int:
            subjectIds = [subjectIds]

        if len(subjectIds) == 1:
            subjectIdStr = "= {}".format(str(subjectIds[0]))
        else:
            subjectIdStr = "IN ({})".format(", ".join([str(x) for x in subjectIds])) 
        
        query = """
        SELECT
            [UserTestSessionId]
            ,[CourseId]
            ,[SubjectId]
            ,[EndedOn]
            ,[TopicId]
            ,[_TotalQuestions]
            ,[_TotalAttempted]
            ,[_TotalCorrect]
            ,[_TotalTimeTaken]
        FROM 
            [speedlabs-anon].[dbo].[UserTestSession]
        WHERE 
            [UserId] = {_userId} AND
            [_TotalQuestions] >= {_minQuestions} AND
            [_TotalTimeTaken] >= {_minTime} AND
            [SubjectId] {_subjectId}
        """.format(_userId = self.userId, _minQuestions = self.config["session"]["minQuestions"], _minTime = self.config["session"]["minTime"],_subjectId = subjectIdStr)
        data = pd.read_sql(query,self.cnxn).set_index("UserTestSessionId")
        
        for subjectId in subjectIds:
            if subjectId not in self.subjects.index:
                self.subjects.loc[subjectId] = Subject(self.userId, "user", subjectId,self.cnxn)
            tempData = data[data["SubjectId"] == subjectId]
            self.subjectData[subjectId, "NoOfSessions"] = tempData.shape[0]
            self.subjectData[subjectId, "TotalNoOfQuestions"] = np.sum(tempData["_TotalQuestions"])
            self.subjectData[subjectId, "TotalTime"] = np.sum(tempData["_TotalQuestions"])

            self.sessionData = self.sessionData.combine_first(data)
            # Generate session objects
            for sessionId in self.sessionData.index:
                if sessionId in self.sessions.index:
                    continue
                self.sessions.loc[sessionId] = Session(self,"user",sessionId, None)
                self.subjects.loc[subjectId].sessions.loc[sessionId] = self.sessions.loc[sessionId]

        return

    def gen_subject_metrics(self, subjectId) -> None:
        self.get_subjects(subjectId)
        self.subjects.loc[subjectId].get_subject_metrics(None)
        return

    def display_metrics(self, sessionId, pseudo = False, gTopic=False):
        self.gen_session_metrics(sessionId, genPseudo=pseudo, genGTopic=gTopic)
        print(self.sessions.loc[sessionId].metrics)
        if pseudo:
            print(self.pseudoSessions.loc[sessionId].metrics)
        if gTopic:
            pass
        return

    def update_topics(self) -> None:

        # Check if sessions are fetched else fetch
        if self.sessionData.shape[0] == 0:
            self.get_sessions()

        # Identify topics from sessions
        topicIds = self.sessionData["TopicId"].unique()
        
        topicDataColumns = [
            "CourseId",
            "SubjectId",
            "TopicId"
        ]

        # Query the database for session data according to topic. 

        for topicId in topicIds:
            if topicId in self.topicData.index:
                continue
        return

    def z_score(self, value, mean, std):
        if int(std) == 0:
            return np.NaN
        return (value - mean)/std

    def compare_session_metrics(self, sessionId, pseudo = False) -> None:
        
        flags = [
            "NoQ",
            "Att",
            "Acc",
            "Time",
            "KSC",
            "Soln",
            "Att2",
            "Acc2",
            "Revw"
        ]
        sessionZScore = pd.DataFrame(columns = flags)
        uM = self.sessions[sessionId].metrics
        if pseudo:
            # Generate Pseudo Session
            self.gen_pseudo_session(sessionId)
            pM = self.pseudoSessions[sessionId].metrics
            for flag in flags:
                sessionZScore.loc["User vs Pseudo",flag] = self.z_score(uM.loc[flag],pM.loc["Avg",flag],pM.loc["Std",flag])
            print(sessionZScore)
        return sessionZScore

    def gen_user_session_history(self, sessionId, historyLength):
        query = """
        SELECT TOP ({_historyLength})
            [UserTestSessionId]
        FROM
            [speedlabs-anon].[dbo].[UserTestSession]
        WHERE
            [UserId] = {_userId} AND
            [EndedOn] < (
                SELECT
                    [StartedOn]
                FROM
                    [speedlabs-anon].[dbo].[UserTestSession]
                WHERE
                    [UserTestSessionId] = {_sessionId}
            ) AND
            [_TotalQuestions] >= {_minQuestions} AND
            [_TotalTimeTaken] >= {_minTime}
        ORDER BY
            [EndedOn] DESC
        """.format(_historyLength = historyLength, _userId = self.userId, _sessionId = sessionId, _minQuestions = self.config["session"]["minQuestions"], _minTime = self.config["session"]["minTime"])
        sessionIds = list(pd.read_sql(query, self.cnxn)["UserTestSessionId"])
        self.gen_session_metrics(sessionIds)
        if self.config["debug"]:
            print("Finished Generating Session Wise Metrics")
        self.sessionHistoryMetrics = pd.Series(dtype=object)
        self.sessionHistoryMetrics.loc[sessionId] = self.gen_user_session_history_metrics(sessionIds)

        return 
        
    def gen_user_session_history_metrics(self, sessionIds):
        columns = ["NoQ", "Att", "Acc", "STim", "ATim", "KSC","Soln", "Att2", "Acc2", "Revw"]
        sessionMetricsData = pd.DataFrame(columns=columns)
        for sessionId in sessionIds:
            session = self.sessions[sessionId]
            sessionMetricsData.loc[session.userSessionId] = session.metrics

        userSessionHistory = pd.DataFrame()
        sD = sessionMetricsData
        userSessionHistory.loc["Avg", "Total Sessions"] = sD.shape[0]
        userSessionHistory.loc["Avg", "NoQ" ], userSessionHistory.loc["Std", "NoQ" ] = (np.mean(sD["NoQ"]), np.std(sD["NoQ"]))
        userSessionHistory.loc["Avg", "Att" ], userSessionHistory.loc["Std", "Att" ] = weighted_avg_and_std(sD["Att"],sD["NoQ"])
        userSessionHistory.loc["Avg", "Acc" ], userSessionHistory.loc["Std", "Acc" ] = weighted_avg_and_std(sD["Acc"] ,sD["NoQ"])
        userSessionHistory.loc["Avg", "ATim"], userSessionHistory.loc["Std", "ATim"] = weighted_avg_and_std(sD["ATim"],sD["NoQ"])
        userSessionHistory.loc["Avg", "STim"], userSessionHistory.loc["Std", "STim"] = (np.mean(sD["STim"]) , np.std(sD["STim"]))
        userSessionHistory.loc["Avg", "KSC" ], userSessionHistory.loc["Std", "KSC" ] = weighted_avg_and_std(sD["KSC" ],sD["NoQ"])
        userSessionHistory.loc["Avg", "Soln"], userSessionHistory.loc["Std", "Soln"] = weighted_avg_and_std(sD["Soln"],sD["NoQ"])
        userSessionHistory.loc["Avg", "Att2"], userSessionHistory.loc["Std", "Att2"] = weighted_avg_and_std(sD["Att2"],sD["NoQ"])
        userSessionHistory.loc["Avg", "Acc2"], userSessionHistory.loc["Std", "Acc2"] = weighted_avg_and_std(sD["Acc2"],sD["NoQ"])
        userSessionHistory.loc["Avg", "Revw"], userSessionHistory.loc["Std", "Revw"] = weighted_avg_and_std(sD["Revw"],sD["NoQ"])

        return userSessionHistory