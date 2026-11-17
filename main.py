# embroidery_io.py
from pyembroidery import read_dst, read, EmbPattern, STITCH, JUMP
from pyembroidery import EmbPattern, STITCH, write_dst
import pyembroidery
import asyncio
import contextlib
import os
from pathlib import Path
from fastapi import UploadFile, HTTPException, status
import tempfile
from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.responses import Response
import requests
from requests.auth import HTTPBasicAuth
import boto3

# --- 1. EMBROIDERY IO & TRANSFORM FUNCTIONS ---

def inspect_stitches(file_path: str) -> list[tuple[int, int, int]]:
    """Read a DST file and return raw stitch data."""
    pattern: EmbPattern = read_dst(file_path)
    return [(s[0], s[1], s[2]) for s in pattern.stitches]

def build_sample_dst(output_path: str) -> None:
    pattern = EmbPattern()
    pattern.add_stitch_absolute(STITCH, 0, 0)
    pattern.add_stitch_relative(STITCH, 10, 5)
    pattern.add_stitch_relative(STITCH, -5, 15)
    write_dst(pattern, output_path)

def convert_embroidery(input_path: str, output_path: str) -> None:
    """Convert any supported embroidery format to another."""
    pyembroidery.convert(input_path, output_path)

def scale_pattern(pattern: EmbPattern, sx: float, sy: float) -> EmbPattern:
    """Scale stitch coordinates by sx / sy factors (in‑place)."""
    for stitch in pattern.stitches:
        stitch[0] = int(round(stitch[0] * sx))
        stitch[1] = int(round(stitch[1] * sy))
    return pattern

def scale_dst(input_path: str, output_path: str, factor: float) -> None:
    pattern = read_dst(input_path)
    scale_pattern(pattern, factor, factor)
    write_dst(pattern, output_path)


# --- 2. UPLOADS & FASTAPI BACKEND ---

_STREAM_CHUNK_BYTES = 1024 * 1024  # 1 MiB

def _stream_to_path_sync(
    file: UploadFile, destination: Path, max_bytes: int
) -> None:
    """Stream UploadFile to destination via an atomic .part file."""
    part = destination.with_suffix(destination.suffix + ".part")
    bytes_written = 0
    try:
        with contextlib.suppress(Exception):
            file.file.seek(0)  # rewind defensively
        with open(part, "wb") as out:
            while True:
                chunk = file.file.read(_STREAM_CHUNK_BYTES)
                if not chunk:
                    break
                bytes_written += len(chunk)
                if bytes_written > max_bytes:
                    raise HTTPException(
                        status_code=status.HTTP_413_CONTENT_TOO_LARGE,
                        detail={
                            "message": "Uploaded file exceeds maximum allowed size",
                            "code": "FILE_SIZE_EXCEEDED",
                        },
                    )
                out.write(chunk)
        os.replace(part, destination)  # atomic rename
    except BaseException:
        if part.exists():
            part.unlink()
        raise

async def stream_to_path(
    file: UploadFile, destination: Path, max_bytes: int
) -> None:
    """Async wrapper — runs blocking I/O in a worker thread."""
    await asyncio.to_thread(
        _stream_to_path_sync, file, destination, max_bytes
    )

app = FastAPI(title="Razi Embroidery API")
ALLOWED_EXT = {".dst", ".emb", ".pes", ".jef", ".exp", ".vp3"}
MAX_BYTES = 50 * 1024 * 1024  # 50 MB

def _convert_sync(
    input_path: str, target_format: str) -> bytes:
    """Blocking conversion — called in a thread pool."""
    output_path = input_path + f".{target_format}"
    try:
        pyembroidery.convert(input_path, output_path)
        with open(output_path, "rb") as f:
            return f.read()
    finally:
        if Path(output_path).exists():
            Path(output_path).unlink()

@app.post("/convert")
async def convert_embroidery_endpoint(
    file: UploadFile = File(...),
    target_format: str = "dst",
):
    ext = Path(file.filename).suffix.lower()
    if ext not in ALLOWED_EXT:
        raise HTTPException(400, f"Unsupported input format: {ext}")
    
    with tempfile.NamedTemporaryFile(
        suffix=ext, delete=False
    ) as tmp:
        in_path = tmp.name
    try:
        await stream_to_path(
            file, Path(in_path), max_bytes=MAX_BYTES
        )
        converted_bytes = await asyncio.to_thread(
            _convert_sync, in_path, target_format
        )
        return Response(
            content=converted_bytes,
            media_type="application/octet-stream",
            headers={
                "Content-Disposition": (
                    f"attachment; filename=converted.{target_format}"
                )
            },
        )
    finally:
        if Path(in_path).exists():
            Path(in_path).unlink()


# --- 3. WOOCOMMERCE AUTOMATION & CATALOG OPTIMIZER ---

WC_URL = os.environ.get("WC_URL", "https://razi-digitizer.com")
WC_KEY = os.environ.get("WC_CONSUMER_KEY", "")
WC_SECRET = os.environ.get("WC_CONSUMER_SECRET", "")
AUTH = HTTPBasicAuth(WC_KEY, WC_SECRET)
HEADERS = {"Content-Type": "application/json"}

def create_downloadable_product(
    name: str,
    price: float,
    file_url: str,
    description: str = "",
) -> dict:
    """
    Create a simple downloadable product.
    Use status='private' to hide it from the catalog.
    """
    endpoint = f"{WC_URL}/wp-json/wc/v3/products"
    payload = {
        "name": name,
        "type": "simple",
        "regular_price": str(price),
        "description": description,
        "downloadable": True,
        "downloads": [
            {"name": "design.dst", "file": file_url}
        ],
        "status": "private",
    }
    resp = requests.post(
        endpoint, json=payload, auth=AUTH, headers=HEADERS
    )
    resp.raise_for_status()
    return resp.json()

def grant_order_download(order_id: int, product: dict) -> None:
    """
    Update the order to completed and add a download permission
    for the product’s first download file.
    """
    download_id = product["downloads"][0]["id"]
    perm_endpoint = (
        f"{WC_URL}/wp-json/wc/v3/orders/{order_id}/downloads"
    )
    payload = {
        "product_id": product["id"],
        "download_id": download_id,
    }
    resp = requests.post(
        perm_endpoint, json=payload, auth=AUTH, headers=HEADERS
    )
    resp.raise_for_status()
    
    order_endpoint = f"{WC_URL}/wp-json/wc/v3/orders/{order_id}"
    requests.put(
        order_endpoint,
        json={"status": "completed"},
        auth=AUTH,
        headers=HEADERS,
    ).raise_for_status()

def optimize_product_tags(
    product_ids: list[int],
    add_tags: list[str],
    wc_url: str,
    auth: HTTPBasicAuth,
) -> None:
    """Append SEO‑friendly tags to existing products."""
    endpoint = f"{wc_url}/wp-json/wc/v3/products"
    for pid in product_ids:
        get_resp = requests.get(
            f"{endpoint}/{pid}", auth=auth
        )
        get_resp.raise_for_status()
        current = get_resp.json()
        existing_tag_ids = {
            t["id"] for t in current.get("tags", [])
        }
        
        new_tag_ids = []
        for tag_name in add_tags:
            tag_resp = requests.post(
                f"{wc_url}/wp-json/wc/v3/products/tags",
                json={"name": tag_name},
                auth=auth,
            )
            if tag_resp.status_code == 201:
                new_tag_ids.append(tag_resp.json()["id"])
            else:
                search = requests.get(
                    f"{wc_url}/wp-json/wc/v3/products/tags",
                    params={"search": tag_name},
                    auth=auth,
                ).json()
                if search:
                    new_tag_ids.append(search[0]["id"])
        
        merged_ids = list(existing_tag_ids | set(new_tag_ids))
        if merged_ids:
            requests.put(
                f"{endpoint}/{pid}",
                json={"tags": [{"id": tid} for tid in merged_ids]},
                auth=auth,
            ).raise_for_status()

def generate_signed_download_url(
    bucket: str,
    key: str,
    expires_in: int = 3600,
) -> str:
    s3 = boto3.client("s3")
    return s3.generate_presigned_url(
        "get_object",
        Params={"Bucket": bucket, "Key": key},
        ExpiresIn=expires_in,
    )
