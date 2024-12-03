from fastapi import FastAPI, Request, Form
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from questions_map import answers

# Initialize FastAPI app and spaCy model
app = FastAPI()
templates = Jinja2Templates(directory="templates")


# NLP-based question processor
def process_question(question):
    return answers(question)


# Web route to handle form input
@app.get("/", response_class=HTMLResponse)
async def get_question_form(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})


@app.post("/submit-question", response_class=HTMLResponse)
async def submit_question(request: Request, question: str = Form(...)):
    response = process_question(question)
    return templates.TemplateResponse("index.html", {"request": request, "response": response})
