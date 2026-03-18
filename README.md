# Signal100 — How to Run

Follow these steps in order every time you want to run the app.

---

## Step 1 — Run the Data Pipeline

Open a terminal and run:

cd Backend
python3 run_pipeline.py

Wait for it to finish before moving on.

---

## Step 2 — Start the Backend

In a terminal, run:

cd /Users/arbindkhaira/Signal100H/signal100-1
source venv/bin/activate
cd Backend
python3 -m uvicorn main:app --reload

Leave this terminal running. You should see:
`Uvicorn running on http://127.0.0.1:8000`

---

## Step 3 — Navigate to the Frontend

Open a **new terminal** and run:

cd signal100_frontend-2
pwd

Confirm the path looks correct before continuing.

---

## Step 4 — Start the Frontend Server

python3 -m http.server 5500

Leave this terminal running.

---

## Step 5 — Open the App

Go to your browser and open:

```
http://localhost:5500
```

---

## Quick Reference

| What | Command |
|---|---|
| Pipeline | `python3 run_pipeline.py` |
| Backend | `python3 -m uvicorn main:app --reload` |
| Frontend | `python3 -m http.server 5500` |
| App URL | `http://localhost:5500` |
| API URL | `http://127.0.0.1:8000` |

---

## Notes

- You need **2 terminals open** at the same time: one for the backend, one for the frontend
- Always activate the venv with `source venv/bin/activate` before running the backend
- If your username is not `arbindkhaira`, replace it in the paths above with your actual Mac username (check with `whoami` in terminal)

---

## ⚡ Performance Tip — Too Slow?

By default, the pipeline runs on **all 500 stocks**. If your Mac is slow, you can limit how many it loads.

In `Backend/data_pipeline.py` on **line 43**, change:

```python
# Default — runs all 500 stocks
csv_files = sorted(RAW_DATA_DIR.glob("*.csv"))
```

To this (add the limit at the end):

```python
csv_files = sorted(RAW_DATA_DIR.glob("*.csv"))[:50]
```

**Pick a number based on your Mac's speed:**

| Your Mac | Recommended |
|---|---|
| Slow / older Mac | `[:50]` |
| Medium speed | `[:100]` |
| Fast / new Mac | `[:200]` |

> The higher the number, the longer it takes. Remove the `[:50]` entirely to go back to all 500.
