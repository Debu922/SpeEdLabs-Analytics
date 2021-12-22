from numpy import string_
from pandas.core.series import Series
import numpy as np

import pyodbc
from pyodbc import Cursor, Connection
import json
from datetime import datetime
import pandas as pd
from IPython.display import display

class DBConnection:
    connection: Connection
    cursor: Cursor

    def __init__(self) -> None:

        with open("D:\Work\SpeEdLabs\SpeEdLabs-Analytics\keys\sql.json","r") as file:
            SQL_key = json.load(file)
        # Some other example server values are
        # server = 'localhost\sqlexpress' # for a named instance
        # server = 'myserver,port' # to specify an alternate port
        server = SQL_key["server"]
        database = SQL_key["database"]
        username = SQL_key["username"]
        password = SQL_key["password"] 

        self.connection = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
        self.cursor = self.connection.cursor()
        
        return

    def get_cursor(self):
        return self.cursor

    def sample_query(self):
        cursor = self.cursor
        cursor.execute("SELECT @@version;") 
        row = cursor.fetchone() 
        while row: 
            print(row[0])
            row = cursor.fetchone()

    def get_test_summary(self, testUserId: int):
        keys = [
            "testId",
            "userId",
            "startedOn",
            "completedOn",
            "pausedOn",
            "status",
            "chapters",
            "topics",
            "totalNoQuestions",
            "totalAttemptedQuestions",
            "totalNotAttemptedQuestions",
            "totalCorrectQuestions",
            "totalIncorrectQuestions",
            "totalTimeTaken",
            "totalTimeTakenCorrect",
            "questionIds"
        ]

        # Query the TestUserId Table
        query = """
        SELECT  
            [TestId]
            ,[UserId]
            ,[StartedOn]
            ,[CompletedOn]
            ,[Status]
        FROM    
            [speedlabs-anon].[dbo].[TestUser]
        WHERE
            [TestUserId] = {x}
        """.format(x=testUserId)
        data = pd.read_sql(query,self.connection)

        testSummary = pd.Series(index=keys)
        testSummary["testId"] = int(data["TestId"][0])
        testSummary["userId"] = int(data["UserId"][0])
        testSummary["status"] = data["Status"][0]
        testSummary["startedOn"] = data["StartedOn"][0]
        testSummary["completedOn"] = data["CompletedOn"][0]

        testSummary["totalTimeTaken"] = testSummary["completedOn"] - testSummary["startedOn"]
        # testSummary[""] = data[""][0]
        # testSummary[""] = data[""][0]
        

        # Query the TestUserQuestion Table
        query = """
        SELECT  
            [_QuestionId]
            ,[_QuestionTypeId]
            ,[_TestSectionId]
            ,[StartedOn]
            ,[CompletedOn]
            ,[TimeTakenInSec]
            ,[IsCorrect]
            ,[_IsAttempted]
            ,[IsMarkedForReview]
        FROM    
            [speedlabs-anon].[dbo].[TestUserQuestion]
        WHERE
            [TestUserId] = {x}
        """.format(x=testUserId)
        data = pd.read_sql(query,self.connection)
        questionType: dict
        questionType["1"] = "Obj"
        questionType["2"] = "Sub"
        questionType["3"] = "MCQ"
        questionType["4"] = "Num"
        questionType["5"] = "AnR"


        testSummary["questionIds"] = data["_QuestionId"]
        testSummary["questionTypes"] = [questionType[str(x)] for x in data["_QuestionTypeId"]]
        testSummary["questionTime"] = data["CompletedOn"] - data["StartedOn"]
        testSummary["questionIsCorrect"] = data["IsCorrect"]
        testSummary["questionIsAttempted"] = data["_IsAttempted"]
        testSummary["questionIsMarkedForReview"] = data["IsMarkedForReview"]

        return testSummary

def get_sessions(userId, dateOnwards, connection):
    query = """
    SELECT 
        [UserTestSessionId]
        ,[CourseId]
        ,[SubjectId]
        ,[TopicId]
        ,[DifficultyLevelId]
        ,[_TotalQuestions]
        ,[_TotalAttempted]
        ,[_TotalCorrect]
    FROM 
        [speedlabs-anon].[dbo].[UserTestSession]
    WHERE 
        [UserId] = {x}
        AND
        [EndedOn] > CONVERT(datetime, '{y}')
    """.format(x = str(userId),y = dateOnwards)
    return pd.read_sql(query, connection)

def get_session_metrics(userTestSessionId, connection):
    """Fetch question data from the SQL server and generate metrics for the session.

    Args:
        userTestSessionId ([type]): [description]
        connection ([type]): [description]

    Returns:
        sessionMetrics Series: 
            ["NoOfQuestions"]               - Total no. of questions in the session.
            ["AttemptedPercentage"]         - Percentage of questions attempted.
            ["KSCViewedPercentage"]         - Percentage of questions for which KSC is viewed.
            ["Accuracy"]                    - Accuracy percentage.
            ["TimeTakenTillFirstSubmission"]- Total time taken in session.
            ["FullSolutionSeenPercentage"]  - Percentage of questions for which full soluton is seen.
            ["AttemptedSecondPercentage"]   - Percentage of incorrectly answered questions for which second attempt made.
            ["AccuracySecond"]              - Accuracy percentage of second try.
            ["MarkedForRevisionPercentage"] - Percentage of qustions marked for revision.
    """

    # Query the UserTestSessionQuestion Table
    query = """
    SELECT 
        [QuestionId]
        ,[IsKSCSeen]
        ,[IsCorrectlyAnswered]
        ,[TimeTakenTillSubmission]
        ,[IsFullSolutionSeen]
        ,[IsCorrectlyAnsweredSecondTime]
        ,[MarkedForRevision]
    FROM 
        [speedlabs-anon].[dbo].[UserTestSessionQuestion]
    WHERE
        [userTestSessionId] = {x}
    """.format(x=userTestSessionId)

    # Fetch data
    sessionData = pd.read_sql(query ,connection)

    # Predefine columns 
    sessionMetricsColumns = [
        "NoOfQuestions",            
        "AttemptedPercentage",      
        "KSCViewedPercentage",        
        "Accuracy",        
        "TimeTaken",
        "FullSolutionSeenPercentage",
        "AttemptedSecondPercentage", 
        "AccuracySecond", 
        "MarkedForRevisionPercentage"
    ]

    sessionMetrics = pd.Series(index = sessionMetricsColumns)

    sessionMetrics["NoOfQuestions"]                = sessionData.shape[0]
    sessionMetrics["AttemptedPercentage"]          = 100.0 * np.sum(sessionData["IsCorrectlyAnswered"].notnull()) / sessionMetrics["NoOfQuestions"]
    sessionMetrics["KSCViewedPercentage"]          = 100.0 * np.sum(sessionData["IsKSCSeen"]) / sessionMetrics["NoOfQuestions"]
    sessionMetrics["Accuracy"]                     = 100.0 * np.sum(sessionData["IsCorrectlyAnswered"]) / sessionMetrics["NoOfQuestions"]
    sessionMetrics["TimeTaken"] = np.sum(sessionData["TimeTakenTillSubmission"])
    sessionMetrics["FullSolutionSeenPercentage"]   = 100.0 * np.sum(sessionData["IsFullSolutionSeen"]) / sessionMetrics["NoOfQuestions"]
    sessionMetrics["AttemptedSecondPercentage"]    = 100.0 * np.sum(sessionData["IsCorrectlyAnsweredSecondTime"].notnull()) / (sessionMetrics["NoOfQuestions"] - np.sum(sessionData["IsCorrectlyAnswered"]))
    sessionMetrics["AccuracySecond"]               = 100.0 * np.sum(sessionData["IsCorrectlyAnsweredSecondTime"]) / (sessionMetrics["NoOfQuestions"] - np.sum(sessionData["IsCorrectlyAnswered"]))
    sessionMetrics["MarkedForRevisionPercentage"]  = 100.0 * np.sum(sessionData["MarkedForRevision"]) / sessionMetrics["NoOfQuestions"]

    return sessionMetrics

def weighted_avg_and_std(values, weights):
    """
    Return the weighted average and standard deviation.

    values, weights -- Numpy ndarrays with the same shape.
    """
    average = np.average(values, weights=weights)
    variance = np.average((values-average)**2, weights=weights)
    return np.array([average, np.sqrt(variance)])


def get_pseudo_session_metrics(userTestSessionId, dateOnwards, connection):
    """[summary]

    Args:
        userTestSessionId ([type]): [description]
        connection ([type]): [description]

    Returns:
        [type]: [description]
    """

    # Query the UserTestSessionQuestion Table to retrieve question data
    query = """
    SELECT 
        [QuestionId]
        ,[IsKSCSeen]
        ,[IsCorrectlyAnswered]
        ,[TimeTakenTillSubmission]
        ,[IsFullSolutionSeen]
        ,[IsCorrectlyAnsweredSecondTime]
        ,[MarkedForRevision]
    FROM 
        [speedlabs-anon].[dbo].[UserTestSessionQuestion]
    WHERE 
        [QuestionId] IN (
            SELECT 
                [QuestionId]
            FROM
                [speedlabs-anon].[dbo].[UserTestSessionQuestion]
            WHERE
                [userTestSessionId] = {x}
        )
        AND
            [CompletedOn] > CONVERT(datetime, '{y}')
    """.format(x = userTestSessionId ,y = dateOnwards)

    # Fetch data
    questionData = pd.read_sql(query,connection)
    # questionData = questionData[questionData["TimeTakenTillSubmission"]]

    questionIds = pd.unique(questionData["QuestionId"])
    # Columns in the pseudoSessionQuestionMetrics
    questionMetricsColumns = [
        "QuestionId",
        "TimesAppeared",
        "AttemptedPercentage",      
        "KSCViewedPercentage",        
        "Accuracy",        
        "AvgTimeTaken",
        "StdTimeTaken",
        "FullSolutionSeenPercentage",
        "AttemptedSecondPercentage", 
        "AccuracySecond", 
        "MarkedForRevisionPercentage"
    ]

    pseudoSessionQuestionMetrics = pd.DataFrame(columns=questionMetricsColumns,index = range(len(questionIds)))
    for idx, questionId in enumerate(questionIds):
        qData = questionData[questionData["QuestionId"]==questionId]

        nTimes = qData.shape[0]
        pseudoSessionQuestionMetrics["QuestionId"][idx]                   = questionId
        pseudoSessionQuestionMetrics["TimesAppeared"][idx]                = nTimes
        pseudoSessionQuestionMetrics["AttemptedPercentage"][idx]          = 100.0 * np.sum(qData["IsCorrectlyAnswered"].notnull()) / nTimes
        pseudoSessionQuestionMetrics["KSCViewedPercentage"][idx]          = 100.0 * np.sum(qData["IsKSCSeen"]) / nTimes
        pseudoSessionQuestionMetrics["Accuracy"][idx]                     = 100.0 * np.sum(qData["IsCorrectlyAnswered"]) / nTimes
        pseudoSessionQuestionMetrics["AvgTimeTaken"][idx]                 = np.mean(qData["TimeTakenTillSubmission"])
        pseudoSessionQuestionMetrics["StdTimeTaken"][idx]                 = np.std(qData["TimeTakenTillSubmission"])
        pseudoSessionQuestionMetrics["FullSolutionSeenPercentage"][idx]   = 100.0 * np.sum(qData["IsFullSolutionSeen"]) / nTimes
        pseudoSessionQuestionMetrics["AttemptedSecondPercentage"][idx]    = 100.0 * np.sum(qData["IsCorrectlyAnsweredSecondTime"].notnull()) / np.sum(~(qData["IsCorrectlyAnswered"].to_numpy(dtype=bool)))
        pseudoSessionQuestionMetrics["AccuracySecond"][idx]               = 100.0 * np.sum(qData["IsCorrectlyAnsweredSecondTime"]) / np.sum(~(qData["IsCorrectlyAnswered"].to_numpy(dtype=bool)))
        pseudoSessionQuestionMetrics["MarkedForRevisionPercentage"][idx]  = 100.0 * np.sum(qData["MarkedForRevision"]) / nTimes

    # Columns in the pseudoSessionMetrics
    sessionMetricsColumns = [
        "NoOfQuestions",            
        "AttemptedPercentage",      
        "KSCViewedPercentage",        
        "Accuracy",        
        "TimeTaken",
        "FullSolutionSeenPercentage",
        "AttemptedSecondPercentage", 
        "AccuracySecond", 
        "MarkedForRevisionPercentage"
    ]

    pseudoSessionMetrics = pd.DataFrame(index = sessionMetricsColumns,columns = ["Mean", "Std"])
    nQuestions = questionIds.shape[0]

    pseudoSessionMetrics.loc["NoOfQuestions","Mean"]       = nQuestions
    pseudoSessionMetrics.loc["NoOfQuestions","Std"]       = 0
    x,y = weighted_avg_and_std(pseudoSessionQuestionMetrics["AttemptedPercentage"],pseudoSessionQuestionMetrics["TimesAppeared"])
    pseudoSessionMetrics.loc["AttemptedPercentage","Mean"] = x
    pseudoSessionMetrics.loc["AttemptedPercentage","Std"] = y
    x,y = weighted_avg_and_std(pseudoSessionQuestionMetrics["KSCViewedPercentage"],pseudoSessionQuestionMetrics["TimesAppeared"])
    pseudoSessionMetrics.loc["KSCViewedPercentage","Mean"] = x
    pseudoSessionMetrics.loc["KSCViewedPercentage","Std"] = y 
    x,y =  weighted_avg_and_std(pseudoSessionQuestionMetrics["Accuracy"],pseudoSessionQuestionMetrics["TimesAppeared"])
    pseudoSessionMetrics.loc["Accuracy","Mean"] = x
    pseudoSessionMetrics.loc["Accuracy","Std"]  = y

    timeMean = np.sum(pseudoSessionQuestionMetrics["AvgTimeTaken"])
    timeStd = np.sqrt(np.sum(pseudoSessionQuestionMetrics["AvgTimeTaken"]**2))

    pseudoSessionMetrics.loc["TimeTaken","Mean"] = timeMean
    pseudoSessionMetrics.loc["TimeTaken","Std"] = timeStd
    x,y = weighted_avg_and_std(pseudoSessionQuestionMetrics["FullSolutionSeenPercentage"],pseudoSessionQuestionMetrics["TimesAppeared"])
    pseudoSessionMetrics.loc["FullSolutionSeenPercentage","Mean"] = x
    pseudoSessionMetrics.loc["FullSolutionSeenPercentage","Std"] = y
    x,y = weighted_avg_and_std(pseudoSessionQuestionMetrics["AttemptedSecondPercentage"],pseudoSessionQuestionMetrics["TimesAppeared"])
    pseudoSessionMetrics.loc["AttemptedSecondPercentage","Mean"] = x
    pseudoSessionMetrics.loc["AttemptedSecondPercentage","Std"]  = y  
    x,y = weighted_avg_and_std(pseudoSessionQuestionMetrics["AccuracySecond"],pseudoSessionQuestionMetrics["TimesAppeared"])
    pseudoSessionMetrics.loc["AccuracySecond","Mean"] = x 
    pseudoSessionMetrics.loc["AccuracySecond","Std"]  = y 
    x,y = weighted_avg_and_std(pseudoSessionQuestionMetrics["MarkedForRevisionPercentage"],pseudoSessionQuestionMetrics["TimesAppeared"])
    pseudoSessionMetrics.loc["MarkedForRevisionPercentage","Mean"] = x
    pseudoSessionMetrics.loc["MarkedForRevisionPercentage","Std"]  = y 

    return pseudoSessionQuestionMetrics, pseudoSessionMetrics

def get_global_topic_metrics(topicId, dateOnwards, connection):
    """Fetch data regarding regarding topicId.

    Args:
        topicId ([type]): [description]
        dateOnwards ([type]): [description]
        connection ([type]): [description]

    Returns:
        [type]: [description]
    """    
    # Query the UserTestSessionQuestion Table to retrieve question data
    query = """
    SELECT 
        [QuestionId]
        ,[IsAttempted]
        ,[IsKSCSeen]
        ,[IsCorrectlyAnswered]
        ,[TimeTakenTillSubmission]
        ,[IsFullSolutionSeen]
        ,[IsCorrectlyAnsweredSecondTime]
        ,[MarkedForRevision]
    FROM 
        [speedlabs-anon].[dbo].[UserTestSessionQuestion]
    WHERE 
        [UserTestSessionId] IN (
            SELECT 
                [UserTestSessionId]
            FROM
                [speedlabs-anon].[dbo].[UserTestSession]
            WHERE
                [TopicId] = {x}
        )
        AND
            [CompletedOn] > CONVERT(datetime, '{y}')
    """.format(x = str(topicId) ,y = dateOnwards)

    # Fetch data
    questionData = pd.read_sql(query,connection)
    # questionData = questionData[questionData["TimeTakenTillSubmission"]]

    questionIds = pd.unique(questionData["QuestionId"])
    # Columns in the pseudoSessionQuestionMetrics
    questionMetricsColumns = [
        "QuestionId",
        "TimesAppeared",
        "AttemptedPercentage",      
        "KSCViewedPercentage",        
        "Accuracy",        
        "AvgTimeTaken",
        "StdTimeTaken",
        "FullSolutionSeenPercentage",
        "AttemptedSecondPercentage", 
        "AccuracySecond", 
        "MarkedForRevisionPercentage"
    ]

    topicMetrics = pd.DataFrame(columns=questionMetricsColumns,index = range(len(questionIds)))
    for idx, questionId in enumerate(questionIds):
        qData = questionData[questionData["QuestionId"]==questionId]
        qData = qData[qData["TimeTakenTillSubmission"]>5]

        nTimes = qData.shape[0]
        if nTimes == 0:
            print(qData)
            continue
        topicMetrics["QuestionId"][idx]                   = questionId
        topicMetrics["TimesAppeared"][idx]                = nTimes
        topicMetrics["AttemptedPercentage"][idx]          = 100.0 * np.sum(qData["IsCorrectlyAnswered"].notnull()) / nTimes
        topicMetrics["KSCViewedPercentage"][idx]          = 100.0 * np.sum(qData["IsKSCSeen"]) / nTimes
        topicMetrics["Accuracy"][idx]                     = 100.0 * np.sum(qData["IsCorrectlyAnswered"]) / nTimes
        topicMetrics["AvgTimeTaken"][idx]                 = np.mean(qData["TimeTakenTillSubmission"])
        topicMetrics["StdTimeTaken"][idx]                 = np.std(qData["TimeTakenTillSubmission"])
        topicMetrics["FullSolutionSeenPercentage"][idx]   = 100.0 * np.sum(qData["IsFullSolutionSeen"]) / nTimes
        topicMetrics["AttemptedSecondPercentage"][idx]    = 100.0 * np.sum(qData["IsCorrectlyAnsweredSecondTime"].notnull()) / np.sum(~(qData["IsCorrectlyAnswered"].to_numpy(dtype=bool)))
        topicMetrics["AccuracySecond"][idx]               = 100.0 * np.sum(qData["IsCorrectlyAnsweredSecondTime"]) / np.sum(~(qData["IsCorrectlyAnswered"].to_numpy(dtype=bool)))
        topicMetrics["MarkedForRevisionPercentage"][idx]  = 100.0 * np.sum(qData["MarkedForRevision"]) / nTimes

    query ="""
    SELECT 
        [_TotalQuestions]
    FROM 
        [speedlabs-anon].[dbo].[UserTestSession]
    WHERE
        [TopicId] = {x}
    """.format(x = topicId)
    questionCountData = pd.read_sql(query,connection)
    questionCountData = questionCountData[questionCountData["_TotalQuestions"]>5]
    # Columns in the pseudoSessionMetrics
    sessionMetricsColumns = [
        "NoOfQuestions",            
        "AttemptedPercentage",      
        "KSCViewedPercentage",        
        "Accuracy",        
        "TimeTaken",
        "FullSolutionSeenPercentage",
        "AttemptedSecondPercentage", 
        "AccuracySecond", 
        "MarkedForRevisionPercentage"
    ]



    topicSessionMetrics = pd.DataFrame(index = sessionMetricsColumns,columns = ["Mean", "Std"])
    nQuestions = questionIds.shape[0]

    topicSessionMetrics.loc["NoOfQuestions","Mean"]       = np.mean(questionCountData["_TotalQuestions"])
    topicSessionMetrics.loc["NoOfQuestions","Std"]       = np.std(questionCountData["_TotalQuestions"])
    x,y = weighted_avg_and_std(topicMetrics["AttemptedPercentage"],topicMetrics["TimesAppeared"])
    topicSessionMetrics.loc["AttemptedPercentage","Mean"] = x
    topicSessionMetrics.loc["AttemptedPercentage","Std"] = y
    x,y = weighted_avg_and_std(topicMetrics["KSCViewedPercentage"],topicMetrics["TimesAppeared"])
    topicSessionMetrics.loc["KSCViewedPercentage","Mean"] = x
    topicSessionMetrics.loc["KSCViewedPercentage","Std"] = y 
    x,y =  weighted_avg_and_std(topicMetrics["Accuracy"],topicMetrics["TimesAppeared"])
    topicSessionMetrics.loc["Accuracy","Mean"] = x
    topicSessionMetrics.loc["Accuracy","Std"]  = y

    timeMean = np.sum(topicMetrics["AvgTimeTaken"])
    timeStd = np.sqrt(np.sum(topicMetrics["AvgTimeTaken"]**2))

    topicSessionMetrics.loc["TimeTaken","Mean"] = timeMean
    topicSessionMetrics.loc["TimeTaken","Std"] = timeStd
    x,y = weighted_avg_and_std(topicMetrics["FullSolutionSeenPercentage"],topicMetrics["TimesAppeared"])
    topicSessionMetrics.loc["FullSolutionSeenPercentage","Mean"] = x
    topicSessionMetrics.loc["FullSolutionSeenPercentage","Std"] = y
    x,y = weighted_avg_and_std(topicMetrics["AttemptedSecondPercentage"],topicMetrics["TimesAppeared"])
    topicSessionMetrics.loc["AttemptedSecondPercentage","Mean"] = x
    topicSessionMetrics.loc["AttemptedSecondPercentage","Std"]  = y  
    x,y = weighted_avg_and_std(topicMetrics["AccuracySecond"],topicMetrics["TimesAppeared"])
    topicSessionMetrics.loc["AccuracySecond","Mean"] = x 
    topicSessionMetrics.loc["AccuracySecond","Std"]  = y 
    x,y = weighted_avg_and_std(topicMetrics["MarkedForRevisionPercentage"],topicMetrics["TimesAppeared"])
    topicSessionMetrics.loc["MarkedForRevisionPercentage","Mean"] = x
    topicSessionMetrics.loc["MarkedForRevisionPercentage","Std"]  = y

    return topicSessionMetrics, topicMetrics 

def get_global_chapter_metrics(questionIds, dateOnWards, config, connection):
    return

def display_nonzero(df):
    df = df.loc[(df!=0).any(axis=1)]
    df = df.loc[:, (df != 0).any(axis=0)]
    display(df)
    return
