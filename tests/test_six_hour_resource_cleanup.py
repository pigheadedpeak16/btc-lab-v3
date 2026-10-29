from btc_v3.storage import db


def test_database_initialization_closes_every_owned_connection(tmp_path,monkeypatch):
    original=db.connect_sqlite;opened=[]
    class Connection:
        def __init__(self,conn):self.conn=conn;self.closed=False
        def __enter__(self):self.conn.__enter__();return self
        def __exit__(self,*args):return self.conn.__exit__(*args)
        def __getattr__(self,name):return getattr(self.conn,name)
        def close(self):self.closed=True;self.conn.close()
    def connect(path):
        wrapper=Connection(original(path));opened.append(wrapper);return wrapper
    monkeypatch.setattr(db,'connect_sqlite',connect)
    try:
        db.initialize_project_databases(tmp_path)
        assert len(opened)==4 and all(c.closed for c in opened),'SQLite transaction context does not close its connection'
    finally:
        for conn in opened:
            if not conn.closed:conn.close()
