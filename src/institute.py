import pandas as pd
from session import Session

class Institute():
    def __init__(self, instituteId, cnxn, config) -> None:

        self.instituteId = instituteId
        
        # Session Metrics
        self.sessionData = pd.DataFrame() # UID, Date, ChapterId, SubjectId
        self.sessions = pd.Series(dtype = object)

        # Chapter Metrics
        self.chapterData = pd.DataFrame() # SubjectId, No of Students, No. of Sessions
        self.shapters = pd.Series(dtype = object)

        # Subject Metrics
        self.subjectData = pd.DataFrame() # No. of Chapers, No. of Sessions
        self.subjects = pd.Series(dtype = object)

        # Users
        self.userIds = pd.Series()

        # SQL Connection
        self.cnxn = cnxn

        # Config
        self.config = config

        return

    def get_sessions(self, sessionIds) -> None:
        if type (sessionIds) == int:
            sessionIds = [sessionIds]
        query = """
        SELECT
            [UserId]
            ,[UserTestSessionId]
            ,[CourseId]
            ,[SubjectId]
            ,[EndedOn]
            ,[_TotalQuestions]
        FROM 
            [speedlabs-anon].[dbo].[UserTestSession]
        WHERE
            [UserTestSessionId] {}
        """
        if len(sessionIds)==1:
            query = query.format("= "+ str(sessionIds[0]))
        else:
            query = query.format("IN (" +", ".join([str(x) for x in sessionIds]) + " )")

        data = pd.read_sql(query,self.cnxn).set_index("UserTestSessionId")

        # Combine data with preexisting data
        self.sessionData = self.sessionData.combine_first(data)

        # Generate session objects
        for sessionId in self.sessionData.index:
            if sessionId in self.sessions.index:
                continue
            self.sessions.loc[sessionId] = Session(self,"pseudo",sessionId, None)
        return

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
    
    def get_subjects(self, chapterIds) -> None:

        return

    def gen_subject_metrics(self) -> None:

        return