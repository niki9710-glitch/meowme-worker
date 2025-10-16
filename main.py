from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import os, subprocess, json, requests, tempfile, shutil

YOUTUBE_API_KEY = os.environ.get("YOUTUBE_API_KEY")
CLOUD_NAME = os.environ.get("CLOUDINARY_CLOUD_NAME")
UPLOAD_PRESET = os.environ.get("CLOUDINARY_UPLOAD_PRESET")

app = FastAPI()

class IngestReq(BaseModel):
    videoId: str
    public_id: str | None = None
    folder: str | None = "meowme/ingest"

def yt_license_is_cc(video_id: str) -> bool:
    url = "https://www.googleapis.com/youtube/v3/videos"
    params = {"id": video_id, "part": "status,snippet", "key": YOUTUBE_API_KEY}
    r = requests.get(url, params=params, timeout=20)
    if r.status_code != 200: return False
    data = r.json()
    items = data.get("items", [])
    if not items: return False
    # YouTube indica CC in "license" (snippet) o in "license" dello status a seconda del caso
    snippet = items[0].get("snippet", {})
    license_info = snippet.get("license")
    return (license_info == "creativeCommon")

def upload_to_cloudinary(file_path: str, public_id: str | None, folder: str | None):
    up_url = f"https://api.cloudinary.com/v1_1/{CLOUD_NAME}/video/upload"
    payload = {"upload_preset": UPLOAD_PRESET}
    if public_id: payload["public_id"] = public_id
    if folder: payload["folder"] = folder
    with open(file_path, "rb") as f:
        files = {"file": f}
        resp = requests.post(up_url, data=payload, files=files, timeout=300)
    if resp.status_code != 200:
        raise HTTPException(status_code=400, detail=resp.text)
    return resp.json()
@app.post("/ingest/youtube")
def ingest_youtube(req: IngestReq, compact: bool = True):
    if not (YOUTUBE_API_KEY and CLOUD_NAME and UPLOAD_PRESET):
        raise HTTPException(status_code=500, detail="Server misconfigured")
    if not yt_license_is_cc(req.videoId):
        raise HTTPException(status_code=403, detail="Video not Creative Commons")

    tmpdir = tempfile.mkdtemp()
    try:
        yurl = f"https://www.youtube.com/watch?v={req.videoId}"
        # Scarica una versione MP4 più "leggera" (<=720p) per velocità/stabilità
        file_out = os.path.join(tmpdir, "%(id)s.%(ext)s")
        cmd = ["yt-dlp", "-f", "mp4[height<=720]/mp4", "-o", file_out, yurl, "-q"]
        subprocess.check_call(cmd)

        # Trova il file
        mp4 = next((os.path.join(tmpdir, n) for n in os.listdir(tmpdir) if n.endswith(".mp4")), None)
        if not mp4:
            raise HTTPException(status_code=400, detail="No MP4 produced")

        # Upload su Cloudinary
        cj = upload_to_cloudinary(mp4, req.public_id, req.folder)

        # --- RISPOSTA SLIM (solo i campi utili all'Action) ---
        resp = {
            "status": "ok",
            "public_id": cj.get("public_id"),
            "secure_url": cj.get("secure_url"),
            "resource_type": cj.get("resource_type"),
            "duration": cj.get("duration"),
            "source": yurl,
            "credits": "Original on YouTube (CC-BY). Include creator/channel link in caption."
        }

        if compact:
            return resp
        else:
            # opzionale: aggiungi info extra ma MAI oggetti enormi
            resp["width"] = cj.get("width")
            resp["height"] = cj.get("height")
            return resp

    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)
