import pandas as pd

class Chapter():
    def __init__(self,studentId, type:str, chapterId:int) -> None:
        self.userId = self.userId
        self.type = type # user/global 
        self.chapterId = chapterId

        self.metrics =  pd.DataFrame()
        self.sessions = pd.Series()
        pass
    
    def update_chapter_progress(self, dateOnWards = None):
        """Fetch all relevant sessions relating to required chapter.

        Args:
            dateOnWards ([type], optional): [description]. Defaults to None.
        """

    def get_chapter_questions(self, sessionIds) -> None:
        pass
