from __future__ import annotations

import json
import os
import re
import time
from typing import Any, Dict, Tuple

from PIL import Image
import pytesseract

os.environ.setdefault("OMP_THREAD_LIMIT", "1")

ROI = Tuple[int, int, int, int]


def _normalize_path(p: str) -> str:
    p = (p or "").strip()
    if not p:
        return p
    p = p.replace("\\", os.sep)
    if not os.path.isabs(p):
        p = os.path.abspath(p)
    return p


def _parse_int(s: str) -> int | None:
    s = (s or "").strip()

    # Normalize common "1" confusions
    s = s.replace("I", "1").replace("l", "1").replace("|", "1")

    # Remove commas/spaces
    s = s.replace(",", "").replace(" ", "")

    m = re.search(r"\d+", s)
    return int(m.group()) if m else None


def _prep_for_tesseract(pil_img: Image.Image, *, scale: int = 3) -> Image.Image:
    import numpy as np
    import cv2

    g = np.array(pil_img.convert("L"))

    if scale and scale > 1:
        g = cv2.resize(g, (g.shape[1] * scale, g.shape[0] * scale), interpolation=cv2.INTER_CUBIC)

    # Light blur helps OCR stability
    g = cv2.GaussianBlur(g, (3, 3), 0)

    # Otsu binarize (digits become dark or light depending; we normalize to black text on white bg)
    _, bw = cv2.threshold(g, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)

    # If background is dark-ish, invert so we always have black digits on white bg
    if (bw == 255).mean() < 0.5:
        bw = 255 - bw

    # ✅ Thicken strokes (helps "11")
    kernel = cv2.getStructuringElement(cv2.MORPH_RECT, (2, 2))
    bw = cv2.dilate(bw, kernel, iterations=1)

    return Image.fromarray(bw)


def _ocr_digits(pil_img: Image.Image, *, psm: int = 7, oem: int = 3) -> str:
    cfg = f"--oem {oem} --psm {psm} -c tessedit_char_whitelist=0123456789Il|"
    return pytesseract.image_to_string(pil_img, config=cfg).strip()


def _prep_for_single_digit(pil_img: Image.Image, *, scale: int = 6) -> Image.Image:
    import numpy as np
    import cv2

    g = np.array(pil_img.convert("L"))

    if scale and scale > 1:
        g = cv2.resize(
            g,
            (g.shape[1] * scale, g.shape[0] * scale),
            interpolation=cv2.INTER_CUBIC,
        )

    # no blur, no dilation: keep the shape of a large 0 intact
    _, bw = cv2.threshold(g, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)

    # normalize to black text on white bg
    if (bw == 255).mean() < 0.5:
        bw = 255 - bw

    return Image.fromarray(bw)


def _parse_single_digit_or_zeroish(s: str) -> int | None:
    s = (s or "").strip()

    if not s:
        return None

    # direct digits first
    m = re.search(r"\d+", s)
    if m:
        return int(m.group())

    # common zero confusions for the wins field
    compact = s.replace(" ", "")
    if compact in {"O", "o", "D", "Q"}:
        return 0

    return None


def _try_read_wins_int(pil_crop: Image.Image) -> tuple[int | None, dict]:
    """
    Specialized fallback for the wins field.
    Helps when a large single '0' is read as O / blob / unknown.
    """
    attempts: list[dict] = []

    # try original crop and isolated crop
    isolated, iso_dbg = _digit_crop_from_components(pil_crop)
    variants = [
        ("orig", pil_crop),
        ("isolated", isolated),
    ]

    best_val: int | None = None
    best_row: dict | None = None

    for label, img in variants:
        prep = _prep_for_single_digit(img, scale=6)

        for psm in (10, 8, 7):
            raw = pytesseract.image_to_string(
                prep,
                config=f"--oem 1 --psm {psm} -c tessedit_char_whitelist=0123456789OoDQ",
            ).strip()

            val = _parse_single_digit_or_zeroish(raw)
            row = {
                "mode": label,
                "psm": psm,
                "raw": raw,
                "value": val,
            }
            attempts.append(row)

            if val is not None:
                best_val = val
                best_row = row
                return best_val, {
                    "isolation": iso_dbg,
                    "best": best_row,
                    "attempts": attempts,
                }

    return None, {
        "isolation": iso_dbg,
        "best": best_row,
        "attempts": attempts,
    }


def _digit_crop_from_components(pil_crop: Image.Image) -> tuple[Image.Image, dict]:
    """
    Tries to isolate digits by connected-component filtering.
    Returns (cropped_image, debug_info). If it can't find digit-like components,
    it returns the original crop.
    """
    import numpy as np
    import cv2

    gray = np.array(pil_crop.convert("L"))
    gray = cv2.GaussianBlur(gray, (3, 3), 0)

    # Otsu -> binary. We want "ink" as 1s.
    _, bw = cv2.threshold(gray, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)

    # Decide whether digits are dark-on-light or light-on-dark by ink amount
    # We want foreground=white(255) for connectedComponents, so invert if needed.
    white_ratio = (bw == 255).mean()
    # If most pixels are white, digits are likely black -> invert so digits become white
    if white_ratio > 0.6:
        fg = 255 - bw
        inverted = True
    else:
        fg = bw
        inverted = False

    # Morph close to connect digit strokes a bit
    kernel = cv2.getStructuringElement(cv2.MORPH_RECT, (2, 2))
    fg = cv2.morphologyEx(fg, cv2.MORPH_CLOSE, kernel, iterations=1)

    num_labels, labels, stats, _ = cv2.connectedComponentsWithStats(fg, connectivity=8)

    H, W = fg.shape[:2]

    # Filter components that look like digits
    candidates = []
    for i in range(1, num_labels):
        x, y, w, h, area = stats[i]
        if area < 20:
            continue

        # digit-ish size constraints (tuned for 1920x1080 UI ROIs)
        if h < int(H * 0.35) or h > int(H * 0.98):
            continue
        if w < 3 or w > int(W * 0.60):
            continue

        # icons are often left-heavy; prefer components in the right ~70% of the ROI
        cx = x + w / 2

        # Prefer right-side components (digits live to the right of the icon)
        if cx < W * 0.35:
            continue

        # Reject very wide blobs (icons / dividers)
        ar = w / max(1, h)
        if ar > 0.9:
            continue

        # Also reject components that touch the left edge (often icon background)
        if x <= 1:
            continue

        candidates.append((x, y, w, h, area))

    dbg = {
        "inverted": inverted,
        "roi_wh": [W, H],
        "components_total": int(num_labels - 1),
        "candidates": len(candidates),
    }

    if not candidates:
        return pil_crop, {**dbg, "used": False}

    # Keep only candidates that are near the rightmost candidate group.
    # This removes stray icon components that pass filters.
    max_cx = max((x + w / 2) for x, y, w, h, a in candidates)
    candidates = [c for c in candidates if (c[0] + c[2] / 2) >= (max_cx - W * 0.35)]


    # Build tight bbox around candidates
    x1 = min(x for x, y, w, h, a in candidates)
    y1 = min(y for x, y, w, h, a in candidates)
    x2 = max(x + w for x, y, w, h, a in candidates)
    y2 = max(y + h for x, y, w, h, a in candidates)

    # Add a little padding
    pad = 8
    x1 = max(0, x1 - pad)
    y1 = max(0, y1 - pad)
    x2 = min(W, x2 + pad)
    y2 = min(H, y2 + pad)

    cropped = pil_crop.crop((x1, y1, x2, y2))
    return cropped, {**dbg, "used": True, "bbox": [int(x1), int(y1), int(x2 - x1), int(y2 - y1)]}


def _prep_hsv_whitecore(pil_img: Image.Image, *, scale: int = 14) -> Image.Image:
    import numpy as np, cv2

    arr = np.array(pil_img.convert("RGB"))
    arr = cv2.resize(arr, (arr.shape[1] * scale, arr.shape[0] * scale), interpolation=cv2.INTER_CUBIC)

    hsv = cv2.cvtColor(arr, cv2.COLOR_RGB2HSV)
    h, s, v = cv2.split(hsv)

    # keep "white-ish" pixels: low saturation + high value
    mask = ((s < 90) & (v > 150)).astype(np.uint8) * 255

    # tesseract prefers black text on white bg
    img = 255 - mask

    # IMPORTANT: do NOT dilate/close here (it merges "11" into one blob)
    return Image.fromarray(img)


def _try_read_int(pil_crop: Image.Image) -> tuple[int | None, dict]:
    """
    Same logic as your working version, but faster:
      - early-exit when we get a "clean digits" read
      - skip redundant whitelist call if no-whitelist already gave clean digits
      - keep HSV fallback only if still None
    """
    attempts: list[dict] = []

    isolated, iso_dbg = _digit_crop_from_components(pil_crop)

    configs = [
        {"psm": 7, "oem": 3, "scale": 3},
        {"psm": 7, "oem": 1, "scale": 3},
    ]

    best_val: int | None = None
    best: dict | None = None
    best_digits_len = -1

    def _norm(s: str) -> str:
        return (s or "").replace("I", "1").replace("l", "1").replace("|", "1")

    def _digits_only(s: str) -> str:
        s = _norm(s)
        return "".join(re.findall(r"\d+", s))

    def consider(mode: str, raw: str, psm: int, oem: int, scale: int) -> tuple[int | None, str, bool]:
        """
        Returns (val, digits, clean)
        clean=True means the OCR output (after normalization) looks like only digits
        (good enough to early-exit safely).
        """
        nonlocal best_val, best, best_digits_len

        raw_norm = _norm(raw)
        digits = "".join(re.findall(r"\d+", raw_norm))
        val = _parse_int(raw)

        # "clean" if, after stripping digits, there's nothing meaningful left
        # (ignoring whitespace). This catches cases like "11" reliably.
        leftover = re.sub(r"[0-9\s]", "", raw_norm)
        clean = (val is not None) and (digits != "") and (leftover == "")

        row = {
            "mode": mode,
            "psm": psm,
            "oem": oem,
            "scale": scale,
            "raw": raw,
            "raw_norm": raw_norm,
            "digits": digits,
            "value": val,
            "clean": clean,
        }
        attempts.append(row)

        if val is None:
            return None, digits, False

        if len(digits) > best_digits_len:
            best_digits_len = len(digits)
            best_val = val
            best = row

        return val, digits, clean

    # 1) Standard attempts with early-exit
    for cfg in configs:
        prep = _prep_for_tesseract(isolated, scale=cfg["scale"])

        # 1a) NO whitelist first
        raw_nowl = pytesseract.image_to_string(
            prep,
            config=f"--oem {cfg['oem']} --psm {cfg['psm']}",
        ).strip()
        val, digits, clean = consider("no_whitelist", raw_nowl, cfg["psm"], cfg["oem"], cfg["scale"])

        # Early-exit if it's clean and not tiny compared to what we could expect.
        # For these UI stats, clean digits is usually correct.
        if clean:
            return val, {"isolation": iso_dbg, "best": best, "attempts": attempts}

        # 1b) Whitelist version is only useful if no-whitelist was messy or empty
        raw_wl = _ocr_digits(prep, psm=cfg["psm"], oem=cfg["oem"])
        val2, digits2, clean2 = consider("whitelist", raw_wl, cfg["psm"], cfg["oem"], cfg["scale"])

        if clean2:
            return val2, {"isolation": iso_dbg, "best": best, "attempts": attempts}

    # 2) HSV white-core fallback (only if nothing worked)
    if best_val is None:
        try:
            prep_hsv = _prep_hsv_whitecore(isolated, scale=14)

            raw_nowl = pytesseract.image_to_string(
                prep_hsv,
                config="--oem 1 --psm 7",
            ).strip()
            val, _, clean = consider("hsv_whitecore_no_whitelist", raw_nowl, 7, 1, 14)
            if clean:
                return val, {"isolation": iso_dbg, "best": best, "attempts": attempts}

            raw_wl = _ocr_digits(prep_hsv, psm=7, oem=1)
            val2, _, clean2 = consider("hsv_whitecore_whitelist", raw_wl, 7, 1, 14)
            if clean2:
                return val2, {"isolation": iso_dbg, "best": best, "attempts": attempts}

        except Exception as e:
            attempts.append({"mode": "hsv_whitecore", "error": str(e)})

    return best_val, {"isolation": iso_dbg, "best": best, "attempts": attempts}

def extract_run_metrics(
    screenshot_path: str,
    rois_for_resolution: Dict[str, Any],
    debug_dir: str | None = None,
    ocr_version: str = "v1",
) -> Dict[str, Any]:
    screenshot_path = _normalize_path(screenshot_path)
    if not os.path.exists(screenshot_path):
        raise FileNotFoundError(f"Screenshot not found: {screenshot_path}")

    im = Image.open(screenshot_path)
    w, h = im.size
    key = f"{w}x{h}"

    if key not in rois_for_resolution:
        raise RuntimeError(f"No OCR ROIs for resolution {key}. Add it to core/ocr_rois.py")

    rois = rois_for_resolution[key]

    debug: Dict[str, Any] = {"resolution": key, "fields": {}}
    out: Dict[str, Any] = {}

    if debug_dir:
        os.makedirs(debug_dir, exist_ok=True)

    for field, roi in rois.items():
        x, y, rw, rh = map(int, roi)
        crop = im.crop((x, y, x + rw, y + rh))

        if debug_dir:
            crop.save(os.path.join(debug_dir, f"{field}_crop.png"))

        val, dbg = _try_read_int(crop)

        # wins is special: a large single "0" is sometimes missed by the generic path
        if field == "wins" and val is None:
            val2, dbg2 = _try_read_wins_int(crop)
            if val2 is not None:
                val = val2
                dbg = {
                    "fallback": "wins_specialized",
                    "generic": dbg,
                    "wins_specialized": dbg2,
                }

        # Save isolated digit region too (super useful)
        if debug_dir:
            try:
                isolated, _ = _digit_crop_from_components(crop)
                isolated.save(os.path.join(debug_dir, f"{field}_digits.png"))
            except Exception:
                pass

        out[field] = val
        debug["fields"][field] = {"roi": [x, y, rw, rh], **dbg}

    wins = out.get("wins")
    out["won"] = bool(wins is not None and wins >= 10)

    out["ocr_json"] = json.dumps(debug, ensure_ascii=False)
    out["ocr_version"] = ocr_version
    out["updated_at_unix"] = int(time.time())
    return out
