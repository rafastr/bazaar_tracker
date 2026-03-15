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

    # Accept only pure digits
    if re.fullmatch(r"\d+", s):
        return int(s)

    return None


def _prep_for_tesseract(
    pil_img: Image.Image,
    *,
    scale: int = 3,
    dilate: bool = True,
) -> Image.Image:
    import numpy as np
    import cv2

    g = np.array(pil_img.convert("L"))

    if scale and scale > 1:
        g = cv2.resize(g, (g.shape[1] * scale, g.shape[0] * scale), interpolation=cv2.INTER_CUBIC)

    g = cv2.GaussianBlur(g, (3, 3), 0)

    _, bw = cv2.threshold(g, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)

    if (bw == 255).mean() < 0.5:
        bw = 255 - bw

    if dilate:
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

    compact = re.sub(r"\s+", "", s)

    # normalize common single-character OCR confusions
    compact = compact.replace("I", "1").replace("l", "1").replace("|", "1")

    # only accept exactly one digit for wins-specialized parsing
    if re.fullmatch(r"\d", compact):
        return int(compact)

    # common zero confusions for the wins field
    if compact in {"O", "o", "D", "Q"}:
        return 0

    return None


def _try_read_wins_int(pil_crop: Image.Image) -> tuple[int | None, dict]:
    """
    Specialized fallback for the wins field.
    Helps when a large single '0' is read as O / blob / unknown.
    Uses multiple OCR attempts and ranks the results instead of returning the
    first parseable value.
    """
    from collections import Counter

    attempts: list[dict] = []

    isolated, iso_dbg = _digit_crop_from_components(pil_crop)
    variants = [
        ("orig", pil_crop),
        ("isolated", isolated),
    ]

    found: list[tuple[int, dict]] = []

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
                found.append((val, row))

    if not found:
        return None, {
            "isolation": iso_dbg,
            "best": None,
            "attempts": attempts,
        }

    counts = Counter(val for val, _ in found)

    ranked = []
    for val, row in found:
        score = (
            counts[val],                                     # more votes is better
            1 if "isolated" in row.get("mode", "") else 0,  # prefer isolated crop
            1 if row.get("psm") in (8, 10) else 0,          # favor single-char-ish modes
            -row.get("psm", 99),
        )
        ranked.append((score, val, row))

    ranked.sort(key=lambda t: t[0], reverse=True)
    _, best_val, best_row = ranked[0]

    return best_val, {
        "isolation": iso_dbg,
        "best": best_row,
        "vote_counts": dict(counts),
        "attempts": attempts,
    }


def _estimate_digit_count_from_isolated_crop(pil_img: Image.Image) -> int | None:
    """
    Estimate whether the isolated crop contains 1 or 2 digits.
    This is intentionally simple and conservative.
    """
    import numpy as np
    import cv2

    gray = np.array(pil_img.convert("L"))
    _, bw = cv2.threshold(gray, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)

    if (bw == 255).mean() > 0.6:
        fg = 255 - bw
    else:
        fg = bw

    num_labels, labels, stats, _ = cv2.connectedComponentsWithStats(fg, connectivity=8)
    H, W = fg.shape[:2]

    comps = []
    for i in range(1, num_labels):
        x, y, w, h, area = stats[i]
        if area < 8:
            continue
        if h < H * 0.45:
            continue
        if w < 2:
            continue
        comps.append((x, y, w, h, area))

    if not comps:
        return None

    comps.sort(key=lambda c: c[0])

    merged = []
    for c in comps:
        if not merged:
            merged.append(list(c))
            continue

        prev = merged[-1]
        prev_x, prev_y, prev_w, prev_h, prev_a = prev
        x, y, w, h, a = c

        gap = x - (prev_x + prev_w)
        if gap <= max(1, int(W * 0.02)):
            x1 = min(prev_x, x)
            y1 = min(prev_y, y)
            x2 = max(prev_x + prev_w, x + w)
            y2 = max(prev_y + prev_h, y + h)
            merged[-1] = [x1, y1, x2 - x1, y2 - y1, prev_a + a]
        else:
            merged.append(list(c))

    count = len(merged)

    if count == 1:
        return 1
    if count == 2:
        return 2

    return None


def _parse_oneish_int(s: str, expected_digits: int | None = None) -> int | None:
    s = (s or "").strip()
    if not s:
        return None

    compact = re.sub(r"\s+", "", s)
    if not compact:
        return None

    compact = compact.replace("I", "1").replace("l", "1").replace("|", "1")

    if re.fullmatch(r"1+", compact):
        if expected_digits is not None:
            return int("1" * expected_digits)
        return int(compact)

    return None


def _try_read_oneish_int(pil_crop: Image.Image) -> tuple[int | None, dict]:
    """
    Specialized fallback for thin values like 1 / 11 in the smaller metadata fields.
    Uses multiple OCR attempts and ranks the results instead of returning the
    first parseable value.
    """
    from collections import Counter

    attempts: list[dict] = []

    isolated, iso_dbg = _digit_crop_from_components(pil_crop)
    expected_digits = _estimate_digit_count_from_isolated_crop(isolated)

    variants = [
        ("isolated", isolated),
        ("orig", pil_crop),
    ]

    found: list[tuple[int, dict]] = []

    for label, img in variants:
        prep = _prep_for_single_digit(img, scale=8)

        for psm in (7, 8, 10):
            raw = pytesseract.image_to_string(
                prep,
                config=f"--oem 1 --psm {psm} -c tessedit_char_whitelist=1Il|",
            ).strip()

            val = _parse_oneish_int(raw, expected_digits=expected_digits)
            row = {
                "mode": label,
                "psm": psm,
                "raw": raw,
                "value": val,
                "expected_digits": expected_digits,
            }
            attempts.append(row)

            if val is not None:
                found.append((val, row))

    if not found:
        return None, {
            "isolation": iso_dbg,
            "best": None,
            "attempts": attempts,
        }

    counts = Counter(val for val, _ in found)

    ranked = []
    for val, row in found:
        digit_len_match = 1 if (
            expected_digits is not None and len(str(val)) == expected_digits
        ) else 0

        score = (
            counts[val],                                     # more votes is better
            digit_len_match,                                 # prefer expected digit count
            1 if "isolated" in row.get("mode", "") else 0,  # prefer isolated crop
            1 if row.get("psm") in (7, 8) else 0,           # prefer normal line/word modes
            -row.get("psm", 99),                             # smaller psm wins as tie-break
        )
        ranked.append((score, val, row))

    ranked.sort(key=lambda t: t[0], reverse=True)
    _, best_val, best_row = ranked[0]

    return best_val, {
        "isolation": iso_dbg,
        "best": best_row,
        "vote_counts": dict(counts),
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

    _, bw = cv2.threshold(gray, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)

    white_ratio = (bw == 255).mean()
    if white_ratio > 0.6:
        fg = 255 - bw
        inverted = True
    else:
        fg = bw
        inverted = False

    kernel = cv2.getStructuringElement(cv2.MORPH_RECT, (2, 2))
    fg = cv2.morphologyEx(fg, cv2.MORPH_CLOSE, kernel, iterations=1)

    num_labels, labels, stats, _ = cv2.connectedComponentsWithStats(fg, connectivity=8)

    H, W = fg.shape[:2]

    candidates = []
    for i in range(1, num_labels):
        x, y, w, h, area = stats[i]
        if area < 12:
            continue

        if h < int(H * 0.30) or h > int(H * 0.98):
            continue
        if w < 2 or w > int(W * 0.75):
            continue

        # reject components glued to the far left edge
        if x <= 1:
            continue

        # allow wider components than before
        ar = w / max(1, h)
        if ar > 1.5:
            continue

        candidates.append((x, y, w, h, area))

    dbg = {
        "inverted": inverted,
        "roi_wh": [W, H],
        "components_total": int(num_labels - 1),
        "candidates_before_grouping": len(candidates),
    }

    if not candidates:
        return pil_crop, {**dbg, "used": False}

    # sort left-to-right
    candidates.sort(key=lambda c: c[0])

    clusters = []
    current = [candidates[0]]

    for c in candidates[1:]:
        prev = current[-1]
        prev_right = prev[0] + prev[2]
        gap = c[0] - prev_right

        if gap <= max(3, int(W * 0.06)):
            current.append(c)
        else:
            clusters.append(current)
            current = [c]

    clusters.append(current)

    # Prefer the cluster with the largest total area.
    # If tied, prefer the rightmost one.
    grouped = max(
        clusters,
        key=lambda cluster: (
            sum(comp[4] for comp in cluster),
            sum(comp[0] + comp[2] / 2 for comp in cluster) / len(cluster),
        ),
    )

    # build bbox around all surviving candidates
    x1 = min(x for x, y, w, h, a in grouped)
    y1 = min(y for x, y, w, h, a in grouped)
    x2 = max(x + w for x, y, w, h, a in grouped)
    y2 = max(y + h for x, y, w, h, a in grouped)

    pad_x = max(4, int(W * 0.03))
    pad_y = max(4, int(H * 0.08))
    x1 = max(0, x1 - pad_x)
    y1 = max(0, y1 - pad_y)
    x2 = min(W, x2 + pad_x)
    y2 = min(H, y2 + pad_y)

    cropped = pil_crop.crop((x1, y1, x2, y2))
    return cropped, {
        **dbg,
        "used": True,
        "candidates_after_grouping": len(grouped),
        "bbox": [int(x1), int(y1), int(x2 - x1), int(y2 - y1)],
    }


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
    Safer OCR for numeric metadata:
      - trust the isolated digit crop first
      - use whitelist-only passes
      - fall back to original crop only if isolated crop fails
    """
    from collections import Counter

    attempts: list[dict] = []

    isolated, iso_dbg = _digit_crop_from_components(pil_crop)
    expected_digits = _estimate_digit_count_from_isolated_crop(isolated)

    def _run_variants(img: Image.Image, label_prefix: str) -> list[tuple[int, dict]]:
        candidates: list[tuple[int, dict]] = []

        variants = [
            (f"{label_prefix}_nodilate", {"scale": 4, "dilate": False}),
            (f"{label_prefix}_dilate", {"scale": 3, "dilate": True}),
            (f"{label_prefix}_big_nodilate", {"scale": 6, "dilate": False}),
        ]

        for mode, prep_cfg in variants:
            prep = _prep_for_tesseract(img, **prep_cfg)

            for psm, oem in ((7, 1), (8, 1), (7, 3)):
                cfg = f"--oem {oem} --psm {psm} -c tessedit_char_whitelist=0123456789Il|"
                raw = pytesseract.image_to_string(prep, config=cfg).strip()
                val = _parse_int(raw)
                if val is not None and expected_digits is not None:
                    # soft filter: reject wildly mismatched lengths
                    if abs(len(str(val)) - expected_digits) >= 2:
                        val = None


                row = {
                    "mode": mode,
                    "psm": psm,
                    "oem": oem,
                    "scale": prep_cfg["scale"],
                    "dilate": prep_cfg["dilate"],
                    "raw": raw,
                    "value": val,
                    "expected_digits": expected_digits,
                }
                attempts.append(row)

                if val is not None:
                    candidates.append((val, row))

        return candidates

    # 1) isolated crop first
    candidates = _run_variants(isolated, "isolated")

    # 2) only if isolated failed completely, try original crop
    if not candidates:
        candidates = _run_variants(pil_crop, "orig")

    # 3) HSV fallback only if still nothing
    if not candidates:
        try:
            prep_hsv = _prep_hsv_whitecore(isolated, scale=14)

            for kind, cfg in (
                ("hsv_psm7", "--oem 1 --psm 7 -c tessedit_char_whitelist=0123456789Il|"),
                ("hsv_psm8", "--oem 1 --psm 8 -c tessedit_char_whitelist=0123456789Il|"),
            ):
                raw = pytesseract.image_to_string(prep_hsv, config=cfg).strip()
                val = _parse_int(raw)

                row = {
                    "mode": kind,
                    "psm": int(kind[-1]),
                    "oem": 1,
                    "scale": 14,
                    "dilate": False,
                    "raw": raw,
                    "value": val,
                }
                attempts.append(row)

                if val is not None:
                    candidates.append((val, row))
        except Exception as e:
            attempts.append({"mode": "hsv_whitecore", "error": str(e)})

    if not candidates:
        return None, {"isolation": iso_dbg, "best": None, "attempts": attempts}

    counts = Counter(val for val, _ in candidates)

    ranked = []
    for val, row in candidates:
        digit_len_match = 1 if (
            expected_digits is not None and len(str(val)) == expected_digits
        ) else 0

        score = (
            counts[val],                                     # more votes is better
            digit_len_match,                                 # prefer expected digit count
            1 if "isolated" in row.get("mode", "") else 0,  # prefer isolated crop
            1 if not row.get("dilate", False) else 0,       # prefer no dilation
            row.get("scale", 0),                            # bigger upscale slightly preferred
        )
        ranked.append((score, val, row))

    ranked.sort(key=lambda t: t[0], reverse=True)
    _, best_val, best_row = ranked[0]

    return best_val, {
        "isolation": iso_dbg,
        "best": best_row,
        "vote_counts": dict(counts),
        "attempts": attempts,
    }


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

        # wins is special: keep it in a plausible range
        wins_suspicious = field == "wins" and (val is None or val < 0 or val > 20)

        if wins_suspicious:
            val2, dbg2 = _try_read_wins_int(crop)
            if val2 is not None:
                val = val2
                dbg = {
                    "fallback": "wins_specialized",
                    "generic": dbg,
                    "wins_specialized": dbg2,
                }

        if field != "wins" and val is None:
            isolated_for_len, _ = _digit_crop_from_components(crop)
            expected_digits = _estimate_digit_count_from_isolated_crop(isolated_for_len)

            if expected_digits in (1, 2):
                val3, dbg3 = _try_read_oneish_int(crop)
                if val3 is not None:
                    val = val3
                    dbg = {
                        "fallback": "oneish_specialized",
                        "generic": dbg,
                        "oneish_specialized": dbg3,
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
