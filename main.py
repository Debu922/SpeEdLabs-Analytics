import os
import sys
import pandas as pd

sys.path.append(os.path.join(os.getcwd(),"src"))

from user import User
from session import Session

from db_util import *

def main():
    pd.set_option('precision', 2)
    student = User(107634)
    student.fetch_top_sessions(1)
    print(student.sessionData)
    sessionId =int( student.sessionData.index[0])
    student.display_metrics(sessionId,pseudo=True)
    student.compare_session_metrics(sessionId,True)

if __name__ == "__main__":
    main()