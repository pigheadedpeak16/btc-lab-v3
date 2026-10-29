import sqlite3
import pytest
from tests.test_registry import make_tournament
from tests.test_stage7_native import legacy,START,END
from btc_v3.experiments import registry
from btc_v3.replay.readers import inspect_native_window
from btc_v3.storage.db import initialize_project_databases


@pytest.mark.parametrize('operation',['native','register','register_existing','load','load_missing'])
def test_short_lived_sqlite_handles_close_on_return_and_exception(tmp_path,monkeypatch,operation):
    paths=initialize_project_databases(tmp_path);cfg=make_tournament()
    ident=registry.register_experiment(paths['forward'],cfg)
    native=tmp_path/'native.sqlite';legacy(native)
    original=sqlite3.connect;opened=[]
    def tracked(*a,**kw):
        conn=original(*a,**kw);opened.append(conn);return conn
    monkeypatch.setattr(sqlite3,'connect',tracked)
    try:
        if operation=='native':inspect_native_window(native,start_us=START,end_us=END)
        elif operation=='register':registry.register_experiment(paths['forward'],make_tournament(45))
        elif operation=='register_existing':registry.register_experiment(paths['forward'],cfg)
        elif operation=='load':assert registry.load_experiment_config(paths['forward'],ident)==cfg
        else:
            with pytest.raises(KeyError):registry.load_experiment_config(paths['forward'],'missing')
        assert opened
        for conn in opened:
            with pytest.raises(sqlite3.ProgrammingError,match='closed'):conn.execute('SELECT 1')
    finally:
        for conn in opened:conn.close()
