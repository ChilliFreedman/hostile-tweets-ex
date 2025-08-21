from fastapi import FastAPI
from manager import Maneser
man = Maneser()


app = FastAPI()

@app.get('/check')
def app1():
    return  "hellooooooooo"

@app.get("/new_df")
def get_json():
    man.run_processor()
    return  man.convert_to_json()
