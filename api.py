from fastapi import FastAPI, File, Form, UploadFile
from parser_utils import run_parser_gpt, run_parser_vlmm
import os
import uvicorn

app = FastAPI()

# Dosyaların kaydedileceği dizin
UPLOAD_FOLDER = "./uploads"
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

@app.post("/api/vllm")
async def gpt_controller(file: UploadFile = File(...), url: str= "https://api.openai.com/v1/chat/completions", query: str = "*", type: str = "schema"):
    # Dosyayı kaydet
    file_path = os.path.join(UPLOAD_FOLDER, file.filename)
    with open(file_path, "wb") as f:
        f.write(await file.read())
    
    # run_parser'ı çağır
    result = run_parser_gpt(file_path, url, query=query, type=type)
    
    return result

@app.post("/api/gpt")
async def gpt_controller(file: UploadFile = File(...), url: str = Form("https://api.openai.com/v1/chat/completions"),
    query: str = Form("*"),
    type: str = Form("schema")):

    # Dosyayı kaydet
    file_path = os.path.join(UPLOAD_FOLDER, file.filename)
    with open(file_path, "wb") as f:
        f.write(await file.read())
    
    # run_parser'ı çağır
    result = run_parser_gpt(file_path, url, query=query, type=type)
    
    return result

if __name__ == "__main__":
    # Uvicorn'u otomatik olarak çalıştır
    uvicorn.run("api:app", host="127.0.0.1", port=5000, reload=True)