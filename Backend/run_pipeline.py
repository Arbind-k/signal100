from train_model import train
from predict import generate_predictions


def run_pipeline():
    print("Starting Signal100 pipeline...")
    train()
    predictions = generate_predictions()
    print(f"Pipeline complete. Generated predictions for {len(predictions)} tickers.")


if __name__ == "__main__":
    run_pipeline()