import json
import cv2

FIELDS = ["wins", "max_health", "prestige", "level", "income", "gold"]

def main(path: str):
    img = cv2.imread(path)
    if img is None:
        raise SystemExit(f"Failed to read image: {path}")

    h, w = img.shape[:2]
    print(f"Image size: {w}x{h}")

    rois = {}
    for field in FIELDS:
        # returns (x, y, w, h)
        r = cv2.selectROI(f"Select ROI: {field} (ENTER=OK, C=cancel)", img, showCrosshair=True, fromCenter=False)
        x, y, rw, rh = map(int, r)
        cv2.destroyWindow(f"Select ROI: {field} (ENTER=OK, C=cancel)")

        if rw == 0 or rh == 0:
            print(f"Skipped {field}")
            continue

        rois[field] = [x, y, rw, rh]
        print(field, "=", rois[field])

    key = f"{w}x{h}"
    out = {key: rois}
    print("\nPaste this into core/ocr_rois.py:\n")
    print(json.dumps(out, indent=2))

    cv2.destroyAllWindows()

if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        raise SystemExit("Usage: python scripts/pick_rois_cv2.py /path/to/RunEnd.png")
    main(sys.argv[1])
