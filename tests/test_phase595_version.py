def test_package_and_execution_phase_identify_the_new_research_release():
    import btc_v3
    from btc_v3.forensics.forward_lab import PHASE_VERSION
    assert PHASE_VERSION=='5.9.5'
    assert btc_v3.__version__=='0.5.9.5'
