from fastapi import FastAPI, Request, Form
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
import spacy

from questions_map import answers

# Initialize FastAPI app and spaCy model
app = FastAPI()
nlp = spacy.load("en_core_web_sm")
templates = Jinja2Templates(directory="templates")


# NLP-based question processor
def process_question(question):
    # Use NLP to parse the question
    doc = nlp(question.lower())
    keywords = [token.lemma_ for token in doc if token.pos_ in ("NOUN", "PROPN")]
    entities = {ent.label_: ent.text for ent in doc.ents}
    # return answers(question.lower())
    return answers(keywords, entities)


# Web route to handle form input
@app.get("/", response_class=HTMLResponse)
async def get_question_form(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})


@app.post("/submit-question", response_class=HTMLResponse)
async def submit_question(request: Request, question: str = Form(...)):
    response = process_question(question)
    return templates.TemplateResponse("index.html", {"request": request, "response": response})
