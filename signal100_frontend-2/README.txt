Signal100 Frontend

Files
- index.html
- styles.css
- app.js

What this frontend does
- Reads all stock predictions from:
  - GET /stocks
  - GET /winners
  - GET /losers
  - GET /model-metrics
  - POST /refresh
- Lets you search and sort predictions
- Shows top winners, top losers, overview stats, and model metrics

How to run

Option 1: simplest
1. Start your FastAPI backend:
   uvicorn main:app --reload

2. In this frontend folder, start a local static server:
   python3 -m http.server 5500

3. Open:
   http://127.0.0.1:5500

4. If needed, set API Base URL in the page to:
   http://127.0.0.1:8000

Option 2: open index.html directly
- This may work, but using a local server is better.

Notes
- Your backend already enables CORS for all origins.
- The frontend expects prediction rows with:
  ticker, current_price, predicted_price, predicted_return, model_mae, model_confidence

Backend endpoints used
- main.py exposes:
  /stocks, /winners, /losers, /model-metrics, /refresh
