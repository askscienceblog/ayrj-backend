# ayrj-backend
Backend API for AYRJ written in Python using FastAPI
# Dependencies
Install the `gcloud` CLI tool and login first. Then,
```
pip install fastapi[all]
pip install google-cloud-firestore
gcloud auth application-default login
```
Afterwards install Google Cloud Storage FUSE to mount the Cloud Storage bucket onto a local folder.
# Documentation
For interactive documentation go to the ```/docs``` path of the API. Note that the automatically generated `POST` for HTTP form data is inaccurate.
# Run
```
uvicorn main:app
```
Now you can access the API via ```http://127.0.0.1:8000/```. This should give you a welcome message.
