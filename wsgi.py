import os, sys
sys.path.append(os.path.dirname(__file__))
from app import app

application = app.wsgifunc()

if __name__ == "__main__":
    app.run()