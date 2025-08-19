import traceback
import time
from fastapi import FastAPI, File, UploadFile, Form, HTTPException
from fastapi.responses import JSONResponse
from services.uploader import upload_erp_data
from services.delete import delete_erp
import requests
import traceback
import os
import tempfile
import os

ALLOWED_EXTENSIONS = {".csv", ".xls", ".xlsx"}

app = FastAPI(title="ERP Data Uploader API", version="1.0.0")

@app.post("/upload-erp/")
async def upload_erp_endpoint(
    user_id: str = Form(...),
    erp_name: str = Form(...),
    files: list[UploadFile] = File(...)  
):
    start_time = time.time()

    if not files or len(files) == 0:
        raise HTTPException(status_code=400, detail="At least one file must be uploaded.")

    results = []
    for file in files:
        ext = os.path.splitext(file.filename)[1].lower()
        if ext not in ALLOWED_EXTENSIONS:
            results.append({
                "file_name": file.filename,
                "status": "error",
                "message": f"Invalid file type '{ext}'. Only CSV, XLS, XLSX are allowed."
            })
            continue  # Skip invalid files

        try:
            # Save file temporarily
            temp_dir = tempfile.mkdtemp()
            file_path = os.path.join(temp_dir, file.filename)
            with open(file_path, "wb") as f:
                f.write(await file.read())

            # Run the ERP data upload logic for each file
            upload_erp_data(user_id, erp_name, file_path)

            results.append({
                "file_name": file.filename,
                "status": "success",
                "message": "ERP data uploaded successfully"
            })

        except Exception as exc:
            error_trace = traceback.format_exc()
            results.append({
                "file_name": file.filename,
                "status": "error",
                "message": str(exc),
                "traceback": error_trace
            })

    execution_time = round(time.time() - start_time, 2)
    return JSONResponse(content={
        "status": "completed",
        "execution_time_seconds": execution_time,
        "results": results
    })

@app.post("/delete-erp/")
async def upload_erp_endpoint(
    user_id: str = Form(...),
    erp_name: str = Form(...),
):
    start_time = time.time()

   
    try:
  # Call your existing logic
        delete_erp(user_id, erp_name)

        execution_time = round(time.time() - start_time, 2)
        return JSONResponse(content={
            "status": "success",
            "message": "ERP data deleted successfully",
            "user_id": user_id,
            "erp_name": erp_name,
            "execution_time_seconds": execution_time
        })

    except Exception as exc:
        execution_time = round(time.time() - start_time, 2)
        error_trace = traceback.format_exc()
        return JSONResponse(content={
            "status": "error",
            "message": str(exc),
            "traceback": error_trace,
            "execution_time_seconds": execution_time
        }, status_code=500)

