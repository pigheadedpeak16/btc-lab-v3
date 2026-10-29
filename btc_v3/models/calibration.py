from __future__ import annotations

import math
from dataclasses import dataclass

import numpy as np


def _clip_probability(p: float) -> float:
    return min(1.0 - 1e-9, max(1e-9, float(p)))


def _logit(p: float) -> float:
    q = _clip_probability(p)
    return math.log(q / (1.0 - q))


def _sigmoid(x: np.ndarray | float) -> np.ndarray | float:
    return 1.0 / (1.0 + np.exp(-x))


@dataclass(frozen=True)
class LogisticCalibrator:
    intercept: float = 0.0
    slope: float = 1.0
    fit_scope: str = "TRAIN_ONLY_REQUIRED"

    def transform(self, probability: float) -> float:
        value = float(_sigmoid(self.intercept + self.slope * _logit(probability)))
        return min(1.0, max(0.0, value))

    @classmethod
    def fit_train_only(
        cls,
        probabilities: list[float],
        labels: list[int],
        *,
        l2: float = 1e-3,
        max_iter: int = 50,
    ) -> "LogisticCalibrator":
        if len(probabilities) != len(labels) or len(probabilities) < 4:
            raise ValueError("probabilities and labels must have equal length >= 4")
        if any(y not in (0, 1) for y in labels):
            raise ValueError("labels must be binary")
        x = np.array([_logit(p) for p in probabilities], dtype=float)
        y = np.array(labels, dtype=float)
        X = np.column_stack([np.ones_like(x), x])
        beta = np.array([0.0, 1.0], dtype=float)
        penalty = np.diag([1e-9, l2])
        for _ in range(max_iter):
            pred = np.asarray(_sigmoid(X @ beta), dtype=float)
            w = np.clip(pred * (1.0 - pred), 1e-6, None)
            h = X.T @ (X * w[:, None]) + penalty
            g = X.T @ (y - pred) - np.array([0.0, l2 * beta[1]])
            delta = np.linalg.solve(h, g)
            beta = beta + delta
            if float(np.max(np.abs(delta))) < 1e-9:
                break
        return cls(intercept=float(beta[0]), slope=float(beta[1]))
