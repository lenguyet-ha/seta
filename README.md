python -m venv venv || python3 -m venv venv
venv\Scripts\activate || source venv/bin/activate
pip install -r requirements.txt
pip install -e ../shared
uvicorn app.main:app --reload

