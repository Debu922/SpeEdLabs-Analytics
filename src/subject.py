
from user import User
import pandas as pd
import numpy as np

class Subject():
    def __init__(self, student:User, type:str, subjectId:int) -> None:
        self.student = student
        self.type = type
        self.subjectId = subjectId

        self.chapterIds = pd.Series()
        self.metrics = pd.DataFrame()
        self.questionData = None
        pass
