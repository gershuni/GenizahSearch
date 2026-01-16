"""
Web Application - Serves HTML templates for the Genizah Corrections UI
This can be run standalone or mounted into the main FastAPI app
"""
from pathlib import Path
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

# Paths
BASE_DIR = Path(__file__).resolve().parent
TEMPLATES_DIR = BASE_DIR / "templates"
STATIC_DIR = BASE_DIR / "static"

# Create app
web_app = FastAPI(title="Genizah Corrections Web UI")

# Mount static files
if STATIC_DIR.exists():
    web_app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

# Templates
templates = Jinja2Templates(directory=str(TEMPLATES_DIR))


# Routes
@web_app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})


@web_app.get("/login", response_class=HTMLResponse)
async def login(request: Request):
    return templates.TemplateResponse("login.html", {"request": request})


@web_app.get("/register", response_class=HTMLResponse)
async def register(request: Request):
    return templates.TemplateResponse("register.html", {"request": request})


@web_app.get("/corrections", response_class=HTMLResponse)
async def corrections_list(request: Request):
    return templates.TemplateResponse("corrections.html", {"request": request})


@web_app.get("/correction/{correction_id}", response_class=HTMLResponse)
async def correction_detail(request: Request, correction_id: int):
    return templates.TemplateResponse("correction_detail.html", {
        "request": request,
        "correction_id": correction_id
    })


@web_app.get("/review", response_class=HTMLResponse)
async def review_page(request: Request):
    return templates.TemplateResponse("review.html", {"request": request})


@web_app.get("/profile/{username}", response_class=HTMLResponse)
async def user_profile(request: Request, username: str):
    return templates.TemplateResponse("profile.html", {
        "request": request,
        "username": username
    })


@web_app.get("/my-corrections", response_class=HTMLResponse)
async def my_corrections(request: Request):
    return templates.TemplateResponse("my_corrections.html", {"request": request})


@web_app.get("/leaderboard", response_class=HTMLResponse)
async def leaderboard(request: Request):
    return templates.TemplateResponse("leaderboard.html", {"request": request})


@web_app.get("/new-correction", response_class=HTMLResponse)
async def new_correction(request: Request):
    return templates.TemplateResponse("new_correction.html", {"request": request})


@web_app.get("/document/{document_id}", response_class=HTMLResponse)
async def document_view(request: Request, document_id: str):
    return templates.TemplateResponse("document.html", {
        "request": request,
        "document_id": document_id
    })


# Run standalone
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(web_app, host="0.0.0.0", port=8080)
