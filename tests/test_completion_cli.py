import json
from pathlib import Path

from btc_v3 import cli
from tests.test_stage7_replay import START,END,events,manifest
from btc_v3.replay.contracts import fingerprint








def test_unfunded_observation_command_requires_authorization_before_io(tmp_path,capsys):
    parser=cli.build_parser()
    assert 'paper-observe-public' in parser._subparsers._group_actions[0].choices, 'Missing safe research bootstrap command'
    args=parser.parse_args(['paper-observe-public','--output',str(tmp_path/'capture.jsonl'),'--watch-wallet','0x'+'1'*40])
    assert cli.cmd_public_observation(args)==3 and not list(tmp_path.iterdir())
