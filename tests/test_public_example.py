"""Exercise the published synthetic example through all retained signal families."""
from decimal import Decimal
import json
from pathlib import Path
import pytest
from btc_v3.replay.engine import replay
from btc_v3.replay.readers import read_jsonl
from btc_v3.strategies.lab_profiles import catalog,verified_definition


@pytest.mark.parametrize('family,expected_fills',[
    ('favourite',1),('fair_value',1),('fair_value_momentum',0),('fair_value_mean_reversion',0),
    ('fair_value_book_imbalance',1),('fair_value_microstructure',1),('fair_value_cross_exchange',0)])
def test_public_example_is_causal_reproducible_and_has_consistent_shadow_accounting(tmp_path,family,expected_fills):
    root=Path(__file__).resolve().parents[1]
    manifest,events=read_jsonl(root/'examples/synthetic-btc-window.jsonl')
    assert manifest.dataset_id=='synthetic-demo-NOT-REAL-MARKET-DATA'
    profile=next(p for p in catalog(tmp_path)['strategies'] if p['config']['strategy_family']==family)
    if family=='fair_value_book_imbalance':
        exported=json.loads((root/'examples/book-imbalance.strategy.json').read_text(encoding='utf-8'))
        assert verified_definition(exported)==verified_definition(profile)
        profile=exported
    result=replay(manifest,events,strategy_definition=profile)
    assert result['status']=='SUPPORTED_SYNTHETIC_OR_CURATED',result['unsupported_reasons']
    assert len(result['fills'])==expected_fills
    assert not {'account','funded_fills','selector_decisions'} & result.keys()
    for row in result['research_rounds']:
        fills=[fill for fill in result['fills'] if fill['strategy']==row['strategy'] and fill['slug']==row['slug']]
        assert row['pnl'] is not None
        # Synthetic outcome is UP: settlement quantity minus actual cost and fees.
        expected=sum((Decimal(fill['shares']) if fill['outcome']=='UP' else Decimal(0) for fill in fills),Decimal(0))
        expected-=sum((Decimal(fill['notional'])+Decimal(fill['fee']) for fill in fills),Decimal(0))
        assert Decimal(row['pnl'])==expected
