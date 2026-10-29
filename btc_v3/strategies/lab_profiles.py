"""Versioned, data-only research definitions. Separate from all experiment ledgers."""
from contextlib import contextmanager
from pathlib import Path
import json,re,sqlite3,uuid
from ..config import StrategyConfig
from ..replay.contracts import fingerprint

PROFILE_POLICY='phase595_shadow_strategy_profiles_v1'
LOCKED_COMMON={'predictor_price_source':'median_composite','settlement_model_type':'twap_monte_carlo',
    'volatility_model':'realized_std','monte_carlo_step_seconds':1.0,'minimum_executable_shares':1.0}


def validate_config(data):
    cfg=StrategyConfig.model_validate(data);c=cfg.common
    if not (1<=c.decision_seconds_before_close<=299 and 0<c.max_quote_age_seconds<=2
        and 100<=c.monte_carlo_paths<=2000 and c.execution_delay_seconds<=10 and c.max_decision_gap_seconds<=10
        and c.shares<=10000 and c.volatility_lookback_seconds<=120):raise ValueError('LAB_RESOURCE_OR_CAUSAL_LIMIT')
    normalized=cfg.model_dump(mode='json')
    if any(normalized['common'][k]!=v for k,v in LOCKED_COMMON.items()):raise ValueError('LAB_UNSUPPORTED_MODEL_SETTING')
    for key,value in normalized['params'].items():
        if key.endswith('_seconds') and value>120:raise ValueError('LAB_HISTORY_LIMIT')
    return cfg


def builtins():
    from ..forensics.forward_lab import full_tournament_specs
    return [dict(strategy_id='BUILTIN_'+name,name=name,revision=1,config=cfg.model_dump(mode='json'),
        config_sha256=fingerprint(cfg.model_dump(mode='json')),frozen=True,profile_policy_revision=PROFILE_POLICY)
        for name,cfg in full_tournament_specs(180) if '_BASE_' in name]


@contextmanager
def connection(root,*,write=False):
    directory=Path(root)/'research/strategy_lab';path=directory/'catalog.sqlite'
    if directory.is_symlink() or path.is_symlink():raise ValueError('LAB_SYMLINK_FORBIDDEN')
    if path.exists() and path.stat().st_size>16_000_000:raise ValueError('LAB_CATALOG_LIMIT')
    if not write and not path.exists():yield None;return
    if write:directory.mkdir(parents=True,exist_ok=True)
    c=sqlite3.connect(path if write else path.resolve().as_uri()+'?mode=ro',uri=not write,timeout=1)
    c.row_factory=sqlite3.Row
    try:
        if write:
            c.executescript('''CREATE TABLE IF NOT EXISTS lab_versions(strategy_id TEXT,revision INTEGER,definition TEXT NOT NULL,
                PRIMARY KEY(strategy_id,revision));CREATE TABLE IF NOT EXISTS lab_heads(strategy_id TEXT PRIMARY KEY,
                revision INTEGER NOT NULL,archived INTEGER NOT NULL DEFAULT 0);''')
            c.execute('BEGIN IMMEDIATE')
        else:c.execute('PRAGMA query_only=ON')
        yield c
        if write:c.commit()
    except BaseException:
        if write:c.rollback()
        raise
    finally:c.close()


def catalog(root):
    strategies=builtins()
    with connection(root) as c:
        if c is not None:
            strategies += [json.loads(r[0]) for r in c.execute('''SELECT v.definition FROM lab_heads h JOIN lab_versions v
                ON h.strategy_id=v.strategy_id AND h.revision=v.revision WHERE h.archived=0 ORDER BY h.strategy_id LIMIT 101''')]
    from ..config import CommonExperimentConfig
    return dict(profile_policy_revision=PROFILE_POLICY,strategies=strategies,locked_common=LOCKED_COMMON,
        common_schema=CommonExperimentConfig.model_json_schema(),
        parameter_schemas={family.value:cls.model_json_schema() for family,cls in StrategyConfig._expected_params().items()},
        safety={'paper_only':True,'launches_collectors':False,'mutates_historical_results':False})


def get_profile(root,strategy_id,*,revision=None):
    for p in builtins():
        if p['strategy_id']==strategy_id:
            if revision not in (None,1):raise ValueError('LAB_REVISION_UNKNOWN')
            return p
    if not re.fullmatch(r'LAB_[0-9a-f]{32}',strategy_id):raise ValueError('LAB_STRATEGY_UNKNOWN')
    with connection(root) as c:
        if c is None:raise ValueError('LAB_STRATEGY_UNKNOWN')
        if revision is None:
            row=c.execute('SELECT revision FROM lab_heads WHERE strategy_id=? AND archived=0',(strategy_id,)).fetchone()
            if row is None:raise ValueError('LAB_STRATEGY_UNKNOWN')
            revision=row[0]
        row=c.execute('SELECT definition FROM lab_versions WHERE strategy_id=? AND revision=?',(strategy_id,revision)).fetchone()
        if row is None:raise ValueError('LAB_REVISION_UNKNOWN')
        return json.loads(row[0])


def save_profile(root,*,name,config,strategy_id=None,expected_revision=None):
    cfg=validate_config(config).model_dump(mode='json')
    if not isinstance(name,str) or not 1<=len(name.strip())<=64 or any(ord(c)<32 for c in name):raise ValueError('LAB_NAME_INVALID')
    if strategy_id is not None and strategy_id.startswith('BUILTIN_'):raise ValueError('LAB_FROZEN_BENCHMARK')
    if strategy_id is not None and not re.fullmatch(r'LAB_[0-9a-f]{32}',strategy_id):raise ValueError('LAB_STRATEGY_UNKNOWN')
    with connection(root,write=True) as c:
        if c.execute('SELECT COUNT(*) FROM lab_versions').fetchone()[0]>=1000:raise ValueError('LAB_VERSION_LIMIT')
        old=None if strategy_id is None else c.execute('SELECT revision,archived FROM lab_heads WHERE strategy_id=?',(strategy_id,)).fetchone()
        if strategy_id is not None and (old is None or old[1] or expected_revision!=old[0]):raise ValueError('LAB_REVISION_CONFLICT')
        if strategy_id is None and c.execute('SELECT COUNT(*) FROM lab_heads WHERE archived=0').fetchone()[0]>=100:raise ValueError('LAB_STRATEGY_LIMIT')
        ident=strategy_id or 'LAB_'+uuid.uuid4().hex;revision=1 if old is None else old[0]+1
        p=dict(strategy_id=ident,name=name.strip(),revision=revision,config=cfg,config_sha256=fingerprint(cfg),
            frozen=False,profile_policy_revision=PROFILE_POLICY)
        c.execute('INSERT INTO lab_versions VALUES(?,?,?)',(ident,revision,json.dumps(p,sort_keys=True,allow_nan=False)))
        c.execute('INSERT OR REPLACE INTO lab_heads VALUES(?,?,0)',(ident,revision))
        return p


def archive_profile(root,strategy_id,*,expected_revision):
    if strategy_id.startswith('BUILTIN_'):raise ValueError('LAB_FROZEN_BENCHMARK')
    with connection(root,write=True) as c:
        row=c.execute('SELECT revision,archived FROM lab_heads WHERE strategy_id=?',(strategy_id,)).fetchone()
        if row is None or row[1] or expected_revision!=row[0]:raise ValueError('LAB_REVISION_CONFLICT')
        c.execute('UPDATE lab_heads SET archived=1 WHERE strategy_id=?',(strategy_id,))
    return {'archived':True,'historical_versions_preserved':True}


def verified_definition(row):
    if not isinstance(row,dict):raise ValueError('LAB_DEFINITION_REQUIRED')
    cfg=validate_config(row['config'])
    if (row.get('profile_policy_revision')!=PROFILE_POLICY or row.get('config_sha256')!=fingerprint(cfg.model_dump(mode='json'))
        or not re.fullmatch(r'(?:LAB_[0-9a-f]{32}|BUILTIN_[A-Z0-9_]+)',row['strategy_id'])):raise ValueError('LAB_DEFINITION_INTEGRITY')
    return cfg
