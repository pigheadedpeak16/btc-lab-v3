"""Single immutable profile evaluated with causal inputs and existing shadow fills."""
from decimal import Decimal
from ..forensics import forward_lab as f
from .lab_profiles import verified_definition


class ConfiguredResearch(f.FullForwardTournament):
    def __init__(self,*,engine,external_store,definition):
        self.definition=definition;self.profile=verified_definition(definition)
        self.strategy=definition['strategy_id'];self.strategies=frozenset({self.strategy});self.tournament=self
        super().__init__(session_id=engine.session_id,store=engine.store,paper_engine=engine,
            external_store=external_store,config=engine.config)

    def observe_book(self,*,collector,now_us):
        market=collector.market
        if self.paper_engine.paired_book_execution_reason(collector,now_us,self.profile.common.max_quote_age_seconds):return
        if self.book_history_slug!=market.slug:self.book_history.clear();self.book_history_slug=market.slug
        if self.book_history and now_us-self.book_history[-1][0]<200_000:return
        book=f.market_book_features(collector.states[market.up_token_id],collector.states[market.down_token_id],
            depth_band=getattr(self.profile.params,'depth_band',.02))
        self.book_history.append((now_us,book))

    def evaluate_due(self,*,market,collector,now_us):
        c=self.profile.common;key=(market.slug,self.strategy);due=market.round_end_us-c.decision_seconds_before_close*1_000_000
        self.market_deadlines[market.slug]=market.round_end_us
        if key in self.seen or now_us<due or now_us>=market.round_end_us:return {}
        if now_us>due+int(c.max_decision_gap_seconds*1e6):
            self.seen.add(key);self._save_skip(strategy=self.strategy,market=market,now_us=now_us,reason='MISSED_DECISION_WINDOW',metadata={})
            return {'SKIP':1}
        if self.paper_engine.paired_book_execution_reason(collector,now_us,c.max_quote_age_seconds):return {}
        try:
            f.PriceReferenceMapper(self.external_store).map_existing_exact(market,now_us)
            windows={5,10,15,30,60}
            windows.update(int(value) for key,value in self.profile.params.model_dump().items() if key.endswith('_seconds'))
            features=self.feature_engine.build(market=market,decision_time_us=now_us,
                return_windows=tuple(sorted(windows)),volatility_windows=tuple(sorted({15,30,60,120,c.volatility_lookback_seconds})))
            model=f.TwapSettlementModel(self.feature_engine,f.SettlementModelConfig(paths=c.monte_carlo_paths,
                volatility_lookback_seconds=c.volatility_lookback_seconds,volatility_floor=c.volatility_floor,volatility_cap=c.volatility_cap,
                seed_version=c.seed_version)).evaluate(market=market,decision_time_us=now_us,features=features)
            book=f.market_book_features(collector.states[market.up_token_id],collector.states[market.down_token_id],
                depth_band=getattr(self.profile.params,'depth_band',.02))
            previous=self._previous_book(now_us=now_us,lookback_seconds=getattr(self.profile.params,'pm_move_lookback_seconds',10),slug=market.slug)
            decision=self.decision_engine.evaluate(self.profile,f.StrategyInputs(btc=features,base_p_up=model.p_up,book=book,
                previous_book=previous,lab=f.build_strategy_lab_features(features)))
        except f.FeatureUnavailable as exc:
            self.input_failures[(market.slug,'CONFIGURED')]=str(exc);return {}
        self.seen.add(key);meta=dict(config_sha256=self.definition['config_sha256'],profile_revision=self.definition['revision'],reason=decision.reason)
        if decision.action is f.Action.SKIP:
            self._save_skip(strategy=self.strategy,market=market,now_us=now_us,reason=decision.reason,metadata=meta);return {'SKIP':1}
        outcome='UP' if decision.action is f.Action.BUY_UP else 'DOWN'
        self._schedule(strategy=self.strategy,cfg=c,market=market,outcome=outcome,
            ask=collector.states[self._token_for(market,outcome)].best_ask.price,now_us=now_us,metadata=meta)
        return {'SCHEDULED':1}

    def next_timer(self,collectors,at_us):
        times=[p.scheduled_at_us for p in self.pending if p.scheduled_at_us>at_us]
        for c in collectors.values():
            if (c.market.slug,self.strategy) in self.seen:continue
            due=c.market.round_end_us-self.profile.common.decision_seconds_before_close*1_000_000
            times.extend(t for t in (due,due+int(self.profile.common.max_decision_gap_seconds*1e6)+1) if t>at_us)
        return min(times) if times else None

    def advance(self,*,collectors,pool,market_store,at_us):
        for c in collectors.values():
            if c.market.round_start_us<=at_us<c.market.round_end_us:
                self.observe_book(collector=c,now_us=at_us);self.evaluate_due(market=c.market,collector=c,now_us=at_us)
        retained=[]
        for p in self.pending:
            if p.scheduled_at_us<=at_us and p.slug not in self.paper_engine.market_fee_rates:
                self.store.update_order_status(p.order_id,status='REJECTED',reason='FEE_UNAVAILABLE')
            else:retained.append(p)
        self.pending=retained
        # All configured profiles use two-sided decision inputs and keep that
        # requirement until execution, independently of the display sample.
        ready=[]
        for p in self.pending:
            reason=None if p.scheduled_at_us>at_us else self.paper_engine.paired_book_execution_reason(pool.collector_for_slug(p.slug),at_us,p.max_quote_age_seconds,market_store=market_store,slug=p.slug)
            if reason:self.store.update_order_status(p.order_id,status='REJECTED',reason=reason)
            else:ready.append(p)
        self.pending=ready
        self.execute_due(collector_pool=pool,market_store=market_store,now_us=at_us)
