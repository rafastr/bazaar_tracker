import json
from PIL import Image
import matplotlib.pyplot as plt
from matplotlib.widgets import RectangleSelector

FIELDS = ["wins", "max_health", "prestige", "level", "income", "gold"]

def main(path: str):
    im = Image.open(path)
    w, h = im.size
    print(f"Image size: {w}x{h}")
    rois = {}

    fig, ax = plt.subplots()
    ax.imshow(im)
    ax.set_title(f"Draw ROI for: {FIELDS[0]}")

    state = {"i": 0}

    def onselect(eclick, erelease):
        x1, y1 = int(eclick.xdata), int(eclick.ydata)
        x2, y2 = int(erelease.xdata), int(erelease.ydata)
        x, y = min(x1, x2), min(y1, y2)
        w_ = abs(x2 - x1)
        h_ = abs(y2 - y1)

        field = FIELDS[state["i"]]
        rois[field] = [x, y, w_, h_]
        print(field, "=", rois[field])

        state["i"] += 1
        if state["i"] >= len(FIELDS):
            plt.close(fig)
        else:
            ax.set_title(f"Draw ROI for: {FIELDS[state['i']]}")
            fig.canvas.draw_idle()

    RectangleSelector(ax, onselect, useblit=True, button=[1], interactive=True)
    plt.show()

    key = f"{w}x{h}"
    out = {key: rois}
    print("\nPaste this into core/ocr_rois.py:\n")
    print(json.dumps(out, indent=2))

if __name__ == "__main__":
    import sys
    main(sys.argv[1])
