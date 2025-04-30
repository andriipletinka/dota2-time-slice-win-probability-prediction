# Dota 2 Time-Slice Win-Probability Prediction

Predicting how likely each team is to win at any moment of a Dota 2 match.  
This repo contains:

* **Replay-parser** – a Java/Maven service built on the Clarity SDK that converts `.dem` replay files into clean JSON snapshots.  
* **Data & ML pipeline** – Python scripts / notebooks that sample high-rank matches from OpenDota, turn the snapshots into fixed-length time slices, and train a Transformer + MLP model to output win probabilities.

Dataset and trained model weights are available on Google Drive:
https://drive.google.com/drive/folders/1Gv8x3imC0x2kM4NMtilYuQ-Pi-2CuMqb?usp=sharing

---

## Prerequisites

| Tool |
|------|
| **Java 8 +** & **Maven** – builds the replay-parser (see `pom.xml`). |
| **Python ≥ 3.10** |
| **CUDA 12.6**-enabled GPU (optional, but the default `torch==2.6.0+cu126` wheels expect it) |

---

## Acknowledgements

**OpenDota** for match data & API.

**Clarity** for the replay parsing library.
