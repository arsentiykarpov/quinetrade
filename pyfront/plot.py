import pandas as pd
import matplotlib
import matplotlib.pyplot as plt

matplotlib.use("agg")

PARQUET_PATH = "test.parquet"

df = pd.read_parquet(PARQUET_PATH)

df["t"] = pd.to_datetime(df["windowEnd"], unit="ms")

fig, axes = plt.subplots(5, 1, figsize=(12, 8), sharex=True)

axes[0].plot(df["t"], df["vwap"])
axes[0].set_ylabel("VWAP")
axes[0].grid(True)

axes[1].plot(df["t"], df["mid"])
axes[1].set_ylabel("Mid")
axes[1].grid(True)

axes[2].plot(df["t"], df["buyShare"])
axes[2].set_ylabel("Buy share")
axes[2].set_xlabel("Time")
axes[2].grid(True)

axes[3].plot(df["t"], df["logRatio"])
axes[3].set_ylabel("Log ratio")
axes[3].set_xlabel("Time")
axes[3].grid(True)

axes[4].plot(df["t"], df["obi"])
axes[4].set_ylabel("OBI")
axes[4].set_xlabel("Time")
axes[4].grid(True)

fig.suptitle("Window metrics")
plt.tight_layout()

out_path = "metrics.png"
plt.savefig(out_path, dpi=150)
print(f"Saved plot to {out_path}")
