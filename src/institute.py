import pandas as pd
import numpy as np
from session import Session
from subject import Subject
from topic import Topic


class Institute():
    def __init__(self, instituteId, cnxn, config) -> None:

        self.instituteId = instituteId

        # Session Metrics
        self.sessionData = pd.DataFrame()  # UID, Date, ChapterId, SubjectId
        self.sessions = pd.Series(dtype=object)

        # Chapter Metrics
        self.chapterData = pd.DataFrame()  # SubjectId, No of Students, No. of Sessions
        self.shapters = pd.Series(dtype=object)

        # Subject Metrics
        self.subjectData = pd.DataFrame()  # No. of Chapers, No. of Sessions
        self.subjects = pd.Series(dtype=object)

        # Topic metrics
        self.topicData = pd.DataFrame()
        self.topic = pd.Series(dtype=object)
        # Users
        self.userIds = pd.Series()

        # SQL Connection
        self.cnxn = cnxn

        # Config
        self.config = config

        return

    def get_sessions(self, sessionIds) -> None:
        if type(sessionIds) == int:
            sessionIds = [sessionIds]
        query = """
        SELECT
            [UserId]
            ,[UserTestSessionId]
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
        if len(sessionIds) == 1:
            query = query.format("= " + str(sessionIds[0]))
        else:
            query = query.format(
                "IN (" + ", ".join([str(x) for x in sessionIds]) + " )")

        data = pd.read_sql(query, self.cnxn).set_index("UserTestSessionId")

        # Combine data with preexisting data
        self.sessionData = self.sessionData.combine_first(data)
        # Generate session objects
        for sessionId in self.sessionData.index:
            if sessionId in self.sessions.index:
                continue
            self.sessions.loc[sessionId] = Session(
                self, "pseudo", sessionId, None)
        return

    def gen_session_metrics(self, sessionIds):

        # Check type of sessionIds
        if type(sessionIds) == int:
            sessionIds = [sessionIds]

        # Fetch sessions if not already fetched
        self.get_sessions(sessionIds=sessionIds)

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
            subjectIdStr = "IN ({})".format(
                ", ".join([str(x) for x in subjectIds]))

        query = """
        SELECT
            [UserTestSessionId]
            ,[UserId]
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
            [_TotalQuestions] >= {_minQuestions} AND
            [_TotalTimeTaken] >= {_minTime} AND
            [SubjectId] {_subjectId} AND
            [EndedOn] > CONVERT(datetime, '{_EndedOn}')
        """.format(_minQuestions=self.config["session"]["minQuestions"], _minTime=self.config["session"]["minTime"], _subjectId=subjectIdStr, _EndedOn=self.config["session"]["dateOnwards"])
        data = pd.read_sql(query, self.cnxn).set_index("UserTestSessionId")
        self.subjectData = pd.DataFrame()
        for subjectId in subjectIds:
            if subjectId not in self.subjects.index:
                self.subjects.loc[subjectId] = Subject(
                    None, "institute", subjectId, self.cnxn)
            tempData = data[data["SubjectId"] == subjectId]
            self.subjectData[subjectId, "NoOfSessions"] = tempData.shape[0]
            self.subjectData[subjectId, "TotalNoOfQuestions"] = np.sum(
                tempData["_TotalQuestions"])
            self.subjectData[subjectId, "TotalTime"] = np.sum(
                tempData["_TotalQuestions"])

            self.sessionData = self.sessionData.combine_first(data)
            # Generate session objects
            for sessionId in self.sessionData.index:
                if sessionId in self.sessions.index:
                    continue
                self.sessions.loc[sessionId] = Session(
                    self, "user", sessionId, None)
                self.subjects.loc[subjectId].sessions.loc[sessionId] = self.sessions.loc[sessionId]
        print(self.subjectData)
        return

    def gen_subject_metrics(self, subjectId) -> None:
        self.get_subjects(subjectId)
        self.subjects.loc[subjectId].get_subject_metrics(self.subjectData)
        return

    def get_topic_metrics(self, topicId):
        query = """
        SELECT
            [UserTestSessionId]
        FROM 
            [speedlabs-anon].[dbo].[UserTestSession]
        WHERE
            [TopicId] = {} AND
            [EndedOn] > CONVERT(datetime, {})
        """.format(topicId, self.config["session"]["dateOnwards"])
        sessionIds = pd.read_sql(query, self.cnxn)["UserTestSessionId"]

        self.get_sessions(sessionIds)
        self.topic.loc[topicId] = Topic(topicId)
        self.topic[topicId].gen_metrics(self.sessionData)
        # return self.topic[topicId].metrics
