from chapter import Chapter
from session import Session
import pandas as pd
from db_util import DBConnection
import numpy as np
import sys

from util import *

class User():
    def __init__(self, userId, cnxn, config, instituteId):
        self.userId = userId
        self.instituteId = userId
        # Session Metrics
        self.sessionData = pd.DataFrame() # UID, Date, ChapterId, SubjectId
        self.sessions = pd.Series(dtype=object)
        
        # Chapter Metrics
        self.chapterData = pd.DataFrame()
        self.chapters = pd.Series(dtype=  object)

        # Subject Metrics
        self.subjectData = pd.DataFrame()
        self.subjects = pd.Series(dtype = object)

        # Topic Metrics
        self.topicData = pd.DataFrame()
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
            "userSession vs userTopic",
            "userSession vs userChapter",
            "userSession vs userSubject",
            "userSession vs globalTopic",
            "userSession vs globalChapter",
            "userSession vs globalSubject"
        ]
        return

    def get_Zscore(self,sessionId, type:str, uM:pd.DataFrame, gM:pd.DataFrame):
        
        # Throw error if incorrect Z Score type
        if type not in self.ZScoresType:
            print("please input correct user type")
            sys.exit(-1)

        # Check if Z Score exists already
        try:
            (sessionId, type) in self.ZScores.index
        except:
            pass
        for label in gM.columns:
            self.ZScores.loc[(sessionId, type),label] = z_score(uM.loc[label],gM.loc["Avg",label],gM.loc["Std",label])
        return
      
    def get_sessions(self, top = 20, sessionIds = None) -> None:
        """Fetch latest session data from database

        Args:
            top (int, optional): [description]. Defaults to 20.
        """
        sessionConfig = self.config["session"]
        
        # sessionFromIDs
        if sessionIds != None:
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

        # Check type of chapterId
        if type(chapterIds) == int:
            chapterIds = [chapterIds]

        
        if len(chapterIds) == 1:
            topicIDstr = "= {}".format(str(chapterIds[0]))
        else:
            topicIDstr = "IN ({})".format(", ".join([str(x) for x in chapterIds])) 
        
        query = """
        SELECT
            [UserTestSessionId]
            ,[CourseId]
            ,[SubjectId]
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
            [_TotalTimeTaken] >= {_minTime} AND
            [TopicId] {_topicId}
        """.format(_userId = self.userId, _minQuestions = self.config["session"]["minQuestions"], _minTime = self.config["session"]["minTime"],_topicId = topicIDstr)
        chapterData = pd.read_sql(query,self.cnxn)
        
        for chapterId in chapterIds:
            if chapterId not in self.chapters.index:
                self.chapters[chapterId] = Chapter("user", chapterId)

        return

    def gen_chapter_metrics(self) -> None:
        return

    def get_subjects(self) -> None:
        return

    def get_subjects(self, subjectIds) -> None:
        return

    def gen_subject_metrics(self) -> None:
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
