python -m venv venv
venv\Scripts\activate
pip install -r requirements.txt
pip install -e ../shared
uvicorn app.main:app --reload

