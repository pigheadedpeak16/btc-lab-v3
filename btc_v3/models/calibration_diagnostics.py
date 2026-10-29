"""Offline chronological, market-level probability diagnostics.

No strategy promotion or online learning. Callers supply verified official labels;
an evidence reference is a provenance requirement, not proof of the caller's claim.
Each cohort freezes revision, source mode and exact forecast horizon. Repeated
markets are rejected rather than counted as independent model samples.
"""
from __future__ import annotations

from collections import Counter, defaultdict
import math

CALIBRATION_POLICY_REVISION = 'stage6_chronological_calibration_v1'
MIN_SPLIT_MARKETS = 100
MIN_BIN_MARKETS = 20
BIN_COUNT = 10


def _bin(p):
    return min(BIN_COUNT - 1, int(p * BIN_COUNT))


def _wilson(wins, n):
    if not n:
        return None
    z = 1.959963984540054
    p = wins / n
    denominator = 1 + z * z / n
    midpoint = (p + z * z / (2 * n)) / denominator
    half = z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n)) / denominator
    return [max(0.0, midpoint - half), min(1.0, midpoint + half)]


def _metrics(pairs):
    if not pairs:
        return None
    n = len(pairs)
    groups = defaultdict(list)
    for p, y in pairs:
        groups[_bin(p)].append((p, y))
    reliability = []
    for i, group in sorted(groups.items()):
        count = len(group)
        wins = sum(y for _, y in group)
        reliability.append(dict(bin=i, markets=count, mean_probability=sum(p for p, _ in group) / count,
            observed_up_rate=wins / count, descriptive_wilson_95=_wilson(wins, count)))
    return dict(markets=n, brier=sum((p - y) ** 2 for p, y in pairs) / n,
        log_loss=-sum(y * math.log(max(1e-15, p)) + (1-y) * math.log(max(1e-15, 1-p)) for p, y in pairs) / n,
        ece=sum(r['markets'] * abs(r['mean_probability']-r['observed_up_rate']) for r in reliability) / n,
        constant_half_brier=.25, reliability=reliability)


def _time(row, key):
    value = row[key]
    if type(value) is not int or value < 0:
        raise ValueError('CALIBRATION_INVALID_TIMESTAMP')
    return value


def calibration_report(samples, *, training_cutoff_us, evaluation_end_us, labels_cutoff_us):
    """Fit smoothed frequency bins on past labels, score a later untouched period.

    All cutoffs and 100-market/20-bin minimums are explicit or frozen constants.
    Sparse bins abstain. Counts are distinct markets, which can still be serially
    dependent; Wilson intervals are descriptive, not a trading-readiness proof.
    """
    if (any(type(t) is not int for t in (training_cutoff_us, evaluation_end_us, labels_cutoff_us))
            or not 0 <= training_cutoff_us < evaluation_end_us <= labels_cutoff_us):
        raise ValueError('CALIBRATION_INVALID_SPLIT_BOUNDARIES')
    cohorts = defaultdict(lambda: {'train': [], 'evaluation': []})
    excluded = Counter()
    seen = set()
    try:
        for row in samples:
            slug = row['slug']
            if slug in seen:
                raise ValueError('CALIBRATION_DUPLICATE_MARKET')
            seen.add(slug)
            start, end, decision = (_time(row, k) for k in
                ('round_start_us', 'round_end_us', 'decision_time_us'))
            if (end - start != 300_000_000 or start % 300_000_000
                    or slug != f'btc-updown-5m-{start // 1_000_000}' or not start <= decision < end
                    or _time(row, 'latest_input_source_ts_us') > decision
                    or _time(row, 'latest_input_received_at_us') > decision):
                raise ValueError('CALIBRATION_NONCAUSAL_OR_INVALID_CONTRACT')
            p = float(row['p_up'])
            sources = tuple(sorted(row['current_sources']))
            revision = row['model_policy_revision']
            if (not math.isfinite(p) or not 0 <= p <= 1 or not isinstance(revision, str) or not revision
                    or not sources or len(set(sources)) != len(sources)
                    or not set(sources) <= {'coinbase', 'kraken'}):
                raise ValueError('CALIBRATION_INVALID_FORECAST_COHORT')
            group = cohorts[(revision, sources, end-decision)]
            outcome = row['outcome']
            if outcome is None:
                excluded['LABEL_NOT_AVAILABLE'] += 1
                continue
            if outcome not in {'UP', 'DOWN'} or not row['resolution_evidence_ref']:
                raise ValueError('CALIBRATION_OFFICIAL_LABEL_REQUIRED')
            observed = _time(row, 'outcome_observed_at_us')
            if observed < end:
                raise ValueError('CALIBRATION_LABEL_BEFORE_EXPIRY')
            if observed > labels_cutoff_us:
                excluded['LABEL_NOT_AVAILABLE'] += 1
                continue
            if decision <= training_cutoff_us:
                if observed > training_cutoff_us:
                    excluded['TRAINING_LABEL_NOT_AVAILABLE'] += 1
                    continue
                group['train'].append((p, int(outcome == 'UP')))
            elif start < training_cutoff_us or decision > evaluation_end_us:
                excluded['OUTSIDE_EVALUATION_WINDOW'] += 1
            else:
                group['evaluation'].append((p, int(outcome == 'UP')))
    except (KeyError, TypeError, OverflowError) as exc:
        raise ValueError('CALIBRATION_MALFORMED_SAMPLE') from exc

    results = []
    for (revision, sources, horizon), data in sorted(cohorts.items()):
        train, evaluation = data['train'], data['evaluation']
        sufficient = len(train) >= MIN_SPLIT_MARKETS and len(evaluation) >= MIN_SPLIT_MARKETS
        fitted = []
        for i in range(BIN_COUNT):
            outcomes = [y for p, y in train if _bin(p) == i]
            n, wins = len(outcomes), sum(outcomes)
            fitted.append(dict(bin=i, markets=n, up_outcomes=wins,
                candidate_probability=(wins+1)/(n+2) if n >= MIN_BIN_MARKETS else None,
                descriptive_wilson_95=_wilson(wins, n)))
        calibrated = [(fitted[_bin(p)]['candidate_probability'], y) for p, y in evaluation
            if sufficient and fitted[_bin(p)]['candidate_probability'] is not None]
        results.append(dict(model_policy_revision=revision, current_sources=list(sources),
            horizon_us=horizon, training_markets=len(train), evaluation_markets=len(evaluation),
            training_bins=fitted, raw_metrics=_metrics(evaluation),
            calibrated_metrics=_metrics(calibrated), calibrated_markets=len(calibrated),
            calibration_abstentions=len(evaluation)-len(calibrated),
            calibration_state='OFFLINE_CANDIDATE_NOT_DEPLOYED' if sufficient else 'INSUFFICIENT_MARKETS'))
    return dict(policy_revision=CALIBRATION_POLICY_REVISION, state='DIAGNOSTICS_ONLY_NOT_PROMOTED',
        training_cutoff_us=training_cutoff_us, evaluation_end_us=evaluation_end_us,
        labels_cutoff_us=labels_cutoff_us, minimum_markets_per_split=MIN_SPLIT_MARKETS,
        minimum_markets_per_bin=MIN_BIN_MARKETS, excluded=dict(excluded), cohorts=results,
        limitations=['Caller must verify official labels and input timestamp provenance.',
            'No independence assumption is established by distinct market counts.',
            'Descriptive intervals omit serial dependence and model-selection uncertainty.',
            'Calibration improvement does not establish executable profitability; no candidate is deployed.'])
