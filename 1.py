from __future__ import annotations
from fastapi import FastAPI, APIRouter, HTTPException, Query, Path
from starlette.responses import JSONResponse, StreamingResponse

app = FastAPI(title="Mock K+ API")
r = APIRouter(prefix="/api/v3.2", tags=["kplus-mock"])

@r.get("/documents/search-by-ref")
async def search_by_ref(ref: str = Query(...)):
    data = SEARCH_BY_REF_RESPONSES.get(ref)
    if not data:
        raise HTTPException(status_code=404, detail=f"no mock for ref={ref}")
    return JSONResponse(data)

@r.get("/documents/{document_id}/state")
async def check_document_status(
    document_id: str = Path(...),
    versioninfo: str = Query(...),
):
    data = STATE_RESPONSES.get((document_id, versioninfo))
    if not data:
        raise HTTPException(
            status_code=404,
            detail=f"no mock for state: id={document_id}, versioninfo={versioninfo}",
        )
    # при необходимости объедините с «служебными» полями
    return JSONResponse({"document": document_id, "versionInfo": versioninfo, **data})

@r.get("/documents/{document_id}/files")
async def get_document_files(document_id: str):
    data = FILES_LIST_RESPONSES.get(document_id)
    if not data:
        raise HTTPException(status_code=404, detail=f"no mock files for id={document_id}")
    return JSONResponse(data)

@r.get("/documents/{document_id}/files/{file_index}")
async def get_file_data(
    document_id: str,
    file_index: int = Path(0, ge=0),
):
    body = FILE_BYTES.get((document_id, file_index))
    if body is None:
        raise HTTPException(status_code=404, detail=f"no mock bytes for id={document_id}, idx={file_index}")

    # выберите правильный content-type
    media = "application/pdf" if file_index == 0 else "application/octet-stream"
    headers = {
        "Content-Disposition": f'inline; filename="mock_{file_index}"'
    }
    return StreamingResponse(iter([body]), media_type=media, headers=headers)

app.include_router(r)
