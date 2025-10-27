# main.py
from __future__ import annotations
import os, json, asyncio, time
from typing import Any, Dict, List, Optional
from fastapi import FastAPI, Request, HTTPException, Header
from pydantic import BaseModel
from zoneinfo import ZoneInfo
from datetime import datetime, timedelta
import httpx

# ---- CONFIG (env) ---------------------------------------------------------
BLOTATO_API_KEY     = os.getenv("blt_PXUUr5bPqSTx57cLFhCDnTM/UCvVWrwEupfoFvKoeZ4=", "")
SPREADSHEET_ID      = os.getenv("1ZVUmJCuxpy_YGCrwknIo52ZXXGha3scMhPhRspfojqM", "")
IG_ACCOUNT_ID       = os.getenv("18329", "")
FB_ACCOUNT_ID       = os.getenv("8852", "")
FB_PAGE_ID          = os.getenv("814075715127747", "")
YT_ACCOUNT_ID       = os.getenv("15639", "")
TIKTOK_ACCOUNT_ID   = os.getenv("19002", "")
OPENAI_API_KEY      = os.getenv("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", "")  # opzionale (migliore qualità)
SERVICE_ACCOUNT_JSON= os.getenv("{
  "type": "service_account",
  "project_id": "meowme-475220",
  "private_key_id": "30d41a84406182a9ee138c2763acd6c3ab1c0bb7",
  "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQDPZlJvlnWyb5hM\nDZUqQLn8DRHD4U9vdALWCxapgVOndZ0x4b6vi+z1rb41EG7oC7NOpnd8pwwX/s7H\n9qLIpBrJAxupcqARNJIlUcT4Q54+6vkS6wSqWe0Yao8g+MMxwOxWAcdSUi/GcOyR\nM63CbU8r9C7pCyqw8Xp1cNncCi/YjuhMpzhEgg3ZYYPVovjBbciUiD+jcNsWH06W\nLq56gzavvRVmwDB1rU4thKsGU1cOo5SiQ+5tctlMlP3Qeey71IyytEq0/J7CkCND\nDgItLaMD2cE3s7jBAYb0WWGGa4wwGPP7+WT86DeQSKhyNfxc1kajTT0ORrLVHWy4\nBQYf3b9VAgMBAAECggEAB9uVlfq6XNdTitYc56vqH/2SxbwcmVlo3hkvc56dv8N6\nADDQW7BOewRpJ3OG8C25TmqKLmPZ1nLl/LeAdV+aUDFY8/OO0vgN7XIKp2lQbovS\ngf/7fkTH8dvfrl0ecB0iFyQcHCdW/gq7O8kcdThU/pJS0KuGo4uSmy56YpbUNz0p\nVYgtDv69//riRCos4QI9I2cKSHc7LY5gSXcJ3uYhpOolHMMjqiVxZZ0/WqRcIekB\niQCa70dTLFtYuR8F0hq5T66uuuFeB5a/mlst/Gy1RWN3BspCdJQIsNqEdBqlURAW\nUKNroleTf1lP4RxmGVYONXMGTQ4p+a0brw6H+l/qpQKBgQD50KPCcwk5M2tcAJ+n\nFI3V+z+L/ThOut0V1BkDgbhgO7qP2BltbWyv1BjmD6aOEqoh1YciqxRII9vcZsxC\nuRkgPDY5FyXcurqzpc4TvvI0NRcgIw3Inpaupof05RGUedeP4jqAUdJt4JCPsSWt\nwAPtDzWccIi2z+KGPYvJag55MwKBgQDUiNk7Itbb/0JSaGbpAjr4gzOzmN4qVEs1\nKn4THEJGn1WD6sw/IvHLFtq1nOdEQ+y4mSwmzzrzWBiYCK+7a/EzGMYycqEnlvX/\nBF+MUbRm2+Gertug3c4AHNLWCeK9SJ4KOYcwyAjqr70gB92PSMdDDOcXsI/KwSy9\nA8g1gLk1VwKBgDwX4VzEOWLGKLw/9ifF/PyNbNLq/eGKd+ZpV/8M9GJJ2+4ASNWW\n7f854Sduel1QxhZPfCttxfo4jgntvJMMXavwcAa5t8TqMkFG0FTVqQPABakYZZdt\n9sdajffuJpV13dHh1LrLc/g0ffHi5jJur0MWVookU+7OASrToU25MnQbAoGAQ8wr\nz8t7Us6Ir5USTv4hJalk0sPPCx28qAcYFKyND85AD0bVRMa27xwpRIn61DH+z5w9\nE1xD5+CQZ99Nf7IdTTl38BvVALYNXv5cJHzj2XQG8wHAmf5nem23bAPXSp5hm81i\nLNOC1Kqe4BriOJT0y5TQGyR9miYpSVptgXPAoR0CgYEA1kIlS+qOXvgkCWYRkHtE\nJsMYRrcBizq9BUZolEF2UFaj3n4R+mJWlGgBrKHyADB+72K0aFi0LY262TdK8NhV\nRkopWQLldKSwF+VjogjlLwyIGO+/6y2eDmtwTBRrEJJMpapC2O3oedQAVnwdAI/B\nXUPgf0+qiP+rOzy/fV8UNI0=\n-----END PRIVATE KEY-----\n",
  "client_email": "meowme-worker@meowme-475220.iam.gserviceaccount.com",
  "client_id": "117034155504950139842",
  "auth_uri": "https://accounts.google.com/o/oauth2/auth",
  "token_uri": "https://oauth2.googleapis.com/token",
  "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
  "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/meowme-worker%40meowme-475220.iam.gserviceaccount.com",
  "universe_domain": "googleapis.com"
}
", "")  # JSON completo della SA
API_SECRET          = os.getenv("Nsdv97_..", "")  # per proteggere l'endpoint

TZ = ZoneInfo("Europe/Zurich")

# ---- APP ------------------------------------------------------------------
app = FastAPI(title="MeowMe Worker", version="1.0.0")

# ---- UTILS ---------------------------------------------------------------
def now_iso_tz() -> str:
    return datetime.now(tz=TZ).isoformat(timespec="seconds")

def today_date() -> str:
    return datetime.now(tz=TZ).date().isoformat()

def coalesce(*vals):
    for v in vals:
        if v not in (None, "", [], {}):
            return v
    return ""

# ---- GOOGLE SHEETS (Service Account) -------------------------------------
# Richiede: SERVICE_ACCOUNT_JSON (contenuto intero del file SA) e condivisione del foglio alla mail della SA
from google.oauth2 import service_account
from google.auth.transport.requests import AuthorizedSession

def _sheet_session() -> AuthorizedSession:
    if not SERVICE_ACCOUNT_JSON:
        raise RuntimeError("GOOGLE_SERVICE_ACCOUNT_JSON mancante")
    info = json.loads(SERVICE_ACCOUNT_JSON)
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = service_account.Credentials.from_service_account_info(info, scopes=scopes)
    return AuthorizedSession(creds)

def sheets_append_row(sheet_id: str, tab_range: str, values: List[List[str]]):
    sess = _sheet_session()
    url = f"https://sheets.googleapis.com/v4/spreadsheets/{sheet_id}/values/{tab_range}:append"
    params = {
        "valueInputOption": "USER_ENTERED",
        "insertDataOption": "INSERT_ROWS"
    }
    body = {"values": values}
    resp = sess.post(url, params=params, json=body)
    if resp.status_code >= 300:
        raise RuntimeError(f"Sheets append error: {resp.status_code} {resp.text}")
    return resp.json()

# ---- OPENAI (facoltativo, per qualità migliori) --------------------------
async def ai_generate(client: httpx.AsyncClient, prompt: str) -> str:
    """
    Se OPENAI_API_KEY è presente, usa OpenAI; altrimenti produce un testo semplice fallback.
    """
    if not OPENAI_API_KEY:
        # Fallback super semplice
        return (
            "TEMA: Perché i gatti fanno lo slow blink?\n"
            "RAZIONALE: Segnale di fiducia e calma; ottimo spunto educativo breve.\n"
            "HASHTAGS: #gatti,#curiositàgatto,#meowme,#catlover,#petfacts\n"
            "SCRIPT_EDU: Hook: Lo slow blink dice 'mi fido di te'. "
            "Spiega come farlo, perché funziona, e CTA finale 'Provalo oggi'.\n"
            "CAPTION_EDU: Sai perché il tuo gatto strizza gli occhi lentamente? 😺\n"
            "FUNNY_STICKER: Quando il salto perfetto… non lo è 😹\n"
            "CAROUSEL_TITOLO: 3 segnali che il tuo gatto ti vuole bene"
        )

    url = "https://api.openai.com/v1/chat/completions"
    headers = {"Authorization": f"Bearer {OPENAI_API_KEY}"}
    data = {
        "model": "gpt-4o-mini",
        "temperature": 0.7,
        "messages": [
            {"role": "system", "content": "Sei un copywriter social per contenuti felini (IT). Rispondi in formato breve, con etichette chiare."},
            {"role": "user", "content": prompt}
        ]
    }
    r = await client.post(url, headers=headers, json=data, timeout=60)
    r.raise_for_status()
    return r.json()["choices"][0]["message"]["content"]

def parse_ai_block(text: str) -> Dict[str, str]:
    # Parser semplice: cerca linee chiave note.
    d: Dict[str, str] = {}
    lines = [l.strip() for l in text.splitlines() if l.strip()]
    for ln in lines:
        upper = ln.upper()
        if upper.startswith("TEMA:"): d["topic"] = ln.split(":",1)[1].strip()
        elif upper.startswith("RAZIONALE:"): d["rationale"] = ln.split(":",1)[1].strip()
        elif upper.startswith("HASHTAGS:"): d["hashtags"] = ln.split(":",1)[1].strip().replace(" ", "")
        elif upper.startswith("SCRIPT_EDU:"): d["script_edu"] = ln.split(":",1)[1].strip()
        elif upper.startswith("CAPTION_EDU:"): d["caption_edu"] = ln.split(":",1)[1].strip()
        elif upper.startswith("FUNNY_STICKER:"): d["funny_sticker"] = ln.split(":",1)[1].strip()
        elif upper.startswith("CAROUSEL_TITOLO:"): d["carousel_title"] = ln.split(":",1)[1].strip()
    # default se mancano
    d.setdefault("topic", "Perché i gatti impastano con le zampe?")
    d.setdefault("rationale", "Gesto legato a benessere e infanzia: perfetto per un breve educational.")
    d.setdefault("hashtags", "#gatti,#curiositàgatto,#meowme,#catlover,#gattini")
    d.setdefault("script_edu", "Hook: L'impastino è un 'ti voglio bene'. Spiega origini e come reagire. CTA: Segui @MeowMe")
    d.setdefault("caption_edu", "Il tuo gatto fa l’impastino? 😺 Scopri perché è un segno d’amore!")
    d.setdefault("funny_sticker", "Quando il salto perfetto… non lo è 😹")
    d.setdefault("carousel_title", "3 segnali che il tuo gatto ti vuole bene")
    return d

# ---- BLOTATO --------------------------------------------------------------
BLOTATO_BASE = "https://backend.blotato.com/v2"
BL_HEADERS = lambda: {"blotato-api-key": BLOTATO_API_KEY, "Content-Type": "application/json"}

async def blotato_create(client: httpx.AsyncClient, payload: Dict[str, Any]) -> Dict[str, Any]:
    r = await client.post(f"{BLOTATO_BASE}/videos/creations", headers=BL_HEADERS(), json=payload, timeout=60)
    if r.status_code >= 300:
        raise HTTPException(status_code=502, detail=f"Blotato create error: {r.status_code} {r.text}")
    return r.json()

async def blotato_status(client: httpx.AsyncClient, id_: str) -> Dict[str, Any]:
    r = await client.get(f"{BLOTATO_BASE}/videos/creations/{id_}", headers=BL_HEADERS(), timeout=60)
    r.raise_for_status()
    return r.json()

async def wait_done(client: httpx.AsyncClient, id_: str, max_wait_s: int = 300, every_s: int = 8) -> Dict[str, Any]:
    t0 = time.time()
    while time.time() - t0 < max_wait_s:
        data = await blotato_status(client, id_)
        st = data.get("item", {}).get("status")
        if st in ("Done", "Failed"):
            return data
        await asyncio.sleep(every_s)
    return {"item": {"status": "Timeout"}}

async def blotato_publish(client: httpx.AsyncClient, account_id: str, platform: str, media_url: str,
                          text: str, target_type: str, media_type: Optional[str] = None, page_id: Optional[str] = None,
                          privacy: Optional[str] = None) -> Dict[str, Any]:
    post = {
        "post": {
            "accountId": account_id,
            "content": {"text": text, "mediaUrls": [media_url], "platform": platform},
            "target": {"targetType": target_type}
        }
    }
    if media_type: post["post"]["target"]["mediaType"] = media_type
    if page_id:    post["post"]["target"]["pageId"]    = page_id
    if privacy:    post["post"]["target"]["privacyLevel"] = privacy
    r = await client.post(f"{BLOTATO_BASE}/posts", headers=BL_HEADERS(), json=post, timeout=60)
    if r.status_code >= 300:
        return {"status": "Failed", "error": r.text}
    return r.json() | {"status": "Queued"}

# ---- Pydantic input -------------------------------------------------------
class RunPayload(BaseModel):
    command: Optional[str] = "meowme_daily"
    timezone: Optional[str] = "Europe/Zurich"
    run_at: Optional[str] = "09:00"

# ---- MAIN FLOW ------------------------------------------------------------
@app.post("/run/meowme")
async def run_meowme(payload: RunPayload, authorization: str | None = Header(default=None)):
    # Security (opzionale)
    if API_SECRET and (authorization or "").replace("Bearer ", "") != API_SECRET:
        raise HTTPException(status_code=401, detail="Unauthorized")

    if not BLOTATO_API_KEY: raise HTTPException(500, "BLOTATO_API_KEY mancante")
    if not SPREADSHEET_ID: raise HTTPException(500, "SPREADSHEET_ID mancante")

    date_iso = now_iso_tz()
    async with httpx.AsyncClient(timeout=60) as client:

        # 1) PLAN (AI → topic/rationale/hashtags/script)
        ai_text = await ai_generate(client, 
            "Genera: TEMA, RAZIONALE, HASHTAGS (5, separati da virgola), SCRIPT_EDU (15-20s), CAPTION_EDU, "
            "FUNNY_STICKER (una riga umoristica per sticker), CAROUSEL_TITOLO. Stile italiano, breve.")
        plan = parse_ai_block(ai_text)

        # Log su Sheets → EditorialPlan
        try:
            sheets_append_row(
                SPREADSHEET_ID, "EditorialPlan!A:E",
                [[date_iso, plan["topic"], plan["rationale"], plan["hashtags"], "AI:OpenAI or Fallback"]]
            )
        except Exception as e:
            # proseguiamo comunque
            pass

        # 2) EDUCATIONAL (Blotato create + wait)
        edu_req = {
            "script": (
                f"GOAL: video verticale 9:16, ~15s, educational, brand MeowMe.\n"
                f"LAYOUT: Intro 1s 'MeowMe' (center) → watermark 'MeowMe' top-right fisso → "
                f"lower-third 1.2–4.5s con testo principale → outro 1s 'Follow @MeowMe'. Safe area.\n"
                f"CONTENT: Tema: {plan['topic']}. {plan['script_edu']}\nCAPTIONS: ON (Italiano)."
            ),
            "style": "cinematic",
            "render": True,
            "voice": {"language": "it", "gender": "female", "tone": "friendly"},
            "overlay": {
                "watermarkText": "MeowMe",
                "lowerThirdText": f"Cat Tip: {plan['topic']}",
                "outroText": "Follow @MeowMe",
                "logoUrl": None
            },
            "durationSec": 15,
            "ratio": "vertical",
            "hashtags": plan["hashtags"]
        }
        edu_crea = await blotato_create(client, edu_req)
        edu_id = edu_crea.get("item", {}).get("id")
        edu_done = await wait_done(client, edu_id)
        edu_url  = coalesce(edu_done.get("item", {}).get("mediaUrl"))
        edu_thumb= coalesce(edu_done.get("item", {}).get("thumbUrl"))
        edu_err  = "" if edu_done.get("item", {}).get("status")=="Done" else str(edu_done)

        # 3) FUNNY (Blotato create + wait)
        funny_req = {
            "script": (
                "GOAL: Clip divertente 9:16, 10–12s. Scene stock/CC0 generiche feline. "
                f"Sticker a 0.5s: '{plan['funny_sticker']}'. Watermark 'MeowMe' top-right. "
                "Musica leggera; evita loghi terzi."
            ),
            "style": "kawaii",
            "render": True,
            "voice": {"language": "it", "gender": "male", "tone": "funny"},
            "overlay": {"watermarkText": "MeowMe","lowerThirdText": None,"outroText": "Follow @MeowMe","logoUrl": None},
            "durationSec": 12,
            "ratio": "vertical",
            "hashtags": "#gatti,#catfails,#funnycats,#MeowMe"
        }
        fun_crea = await blotato_create(client, funny_req)
        fun_id   = fun_crea.get("item", {}).get("id")
        fun_done = await wait_done(client, fun_id)
        fun_url  = coalesce(fun_done.get("item", {}).get("mediaUrl"))
        fun_thumb= coalesce(fun_done.get("item", {}).get("thumbUrl"))
        fun_err  = "" if fun_done.get("item", {}).get("status")=="Done" else str(fun_done)

        # 4) CAROUSEL (slideshow video 15s)
        car_req = {
            "script": (
                f"GOAL: Slideshow 3–5 scene (video) in 15s.\nCover: '{plan['carousel_title']}'. "
                "Slide2: Slow Blink = fiducia; Slide3: Head bunting = confidenza; "
                "Slide4: Kneading = comfort; Slide5 CTA: 'Segui @MeowMe'. "
                "Brand scuro, testo grande, watermark 'MeowMe' top-right."
            ),
            "style": "cinematic",
            "render": True,
            "voice": {"language": "it", "gender": "female", "tone": "calm"},
            "overlay": {"watermarkText": "MeowMe","lowerThirdText": None,"outroText": "Follow @MeowMe","logoUrl": None},
            "durationSec": 15,
            "ratio": "vertical",
            "hashtags": "#gatti,#MeowMe,#catbehaviour,#carousel"
        }
        car_crea = await blotato_create(client, car_req)
        car_id   = car_crea.get("item", {}).get("id")
        car_done = await wait_done(client, car_id)
        car_url  = coalesce(car_done.get("item", {}).get("mediaUrl"))
        car_thumb= coalesce(car_done.get("item", {}).get("thumbUrl"))
        car_err  = "" if car_done.get("item", {}).get("status")=="Done" else str(car_done)

        # 5) LOG OUTPUTS
        try:
            sheets_append_row(SPREADSHEET_ID,"Outputs!A:I",
                [[date_iso,"Educational","IG;FB;YT;TikTok", edu_url, edu_thumb, "15", plan["caption_edu"], plan["hashtags"], edu_err]])
            sheets_append_row(SPREADSHEET_ID,"Outputs!A:I",
                [[date_iso,"Funny","IG;FB;YT;TikTok", fun_url, fun_thumb, "12", "Quando il salto perfetto… non lo è 😹", "#gatti,#catfails,#funnycats,#MeowMe", fun_err]])
            sheets_append_row(SPREADSHEET_ID,"Outputs!A:I",
                [[date_iso,"Carousel","IG Feed", car_url, car_thumb, "15", plan["carousel_title"], "#gatti,#MeowMe,#catbehaviour,#carousel", car_err]])
        except Exception:
            pass

        # 6) PUBLISH (se ho URL)
        pub_rows = []
        if edu_url:
            ig = await blotato_publish(client, IG_ACCOUNT_ID, "instagram", edu_url, f"🐾 {plan['topic']}\n{plan['rationale']}\n\n{plan['hashtags']}", "instagram", "reel")
            fb = await blotato_publish(client, FB_ACCOUNT_ID, "facebook",  edu_url, f"🐾 {plan['topic']}\n{plan['rationale']}\n\n{plan['hashtags']}", "facebook",  "reel", page_id=FB_PAGE_ID)
            yt = await blotato_publish(client, YT_ACCOUNT_ID, "youtube",   edu_url, f"MeowMe — {plan['topic']} #shorts\n{plan['hashtags']}", "youtube",   "short")
            tt = await blotato_publish(client, TIKTOK_ACCOUNT_ID, "tiktok", edu_url, f"{plan['topic']} 😺\n{plan['hashtags']}", "tiktok", privacy="PUBLIC_TO_EVERYONE")
            pub_rows += [
                ["Instagram","Educational", ig.get("status","") ,"",""],
                ["Facebook" ,"Educational", fb.get("status","") ,"",""],
                ["YouTube"  ,"Educational", yt.get("status","") ,"",""],
                ["TikTok"   ,"Educational", tt.get("status","") ,"",""],
            ]

        if fun_url:
            ig = await blotato_publish(client, IG_ACCOUNT_ID, "instagram", fun_url, "Quando il salto perfetto… non lo è 😹\n#gatti,#catfails,#funnycats,#MeowMe", "instagram", "reel")
            fb = await blotato_publish(client, FB_ACCOUNT_ID, "facebook",  fun_url, "Quando il salto perfetto… non lo è 😹\n#gatti,#catfails,#funnycats,#MeowMe", "facebook",  "reel", page_id=FB_PAGE_ID)
            yt = await blotato_publish(client, YT_ACCOUNT_ID, "youtube",   fun_url, "Quando il salto perfetto… non lo è 😹 #shorts\n#gatti,#catfails,#funnycats,#MeowMe", "youtube",   "short")
            tt = await blotato_publish(client, TIKTOK_ACCOUNT_ID, "tiktok", fun_url, "Quando il salto perfetto… non lo è 😹\n#gatti,#catfails,#funnycats,#MeowMe", "tiktok", privacy="PUBLIC_TO_EVERYONE")
            pub_rows += [
                ["Instagram","Funny", ig.get("status","") ,"",""],
                ["Facebook" ,"Funny", fb.get("status","") ,"",""],
                ["YouTube"  ,"Funny", yt.get("status","") ,"",""],
                ["TikTok"   ,"Funny", tt.get("status","") ,"",""],
            ]

        # publish log
        if pub_rows:
            try:
                for r in pub_rows:
                    sheets_append_row(SPREADSHEET_ID,"PublishLog!A:F", [[date_iso] + r])
            except Exception:
                pass

        # 7) DAILY REPORT
        try:
            sheets_append_row(SPREADSHEET_ID,"DailyReport!A:J", [[
                date_iso, plan["topic"], edu_url, fun_url, car_url,
                pub_rows[0][2] if pub_rows else "",  # IG edu status (approx)
                "", "", "",  # FB/YT/TT flags opzionali qui
                f"EDU_ERR:{edu_err or 'none'} | FUN_ERR:{fun_err or 'none'} | CAR_ERR:{car_err or 'none'}"
            ]])
        except Exception:
            pass

    return {
        "status": "ok",
        "date": date_iso,
        "topic": plan["topic"],
        "edu_url": edu_url,
        "fun_url": fun_url,
        "car_url": car_url
    }

