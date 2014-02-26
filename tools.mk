test: compile
	./rebar eunit skip_deps=true

docs:
	./rebar doc skip_deps=true

PLT ?= $(HOME)/.riak_combo_dialyzer_plt
LOCAL_PLT = .local_dialyzer_plt
DIALYZER_FLAGS ?= -Wunmatched_returns

${PLT}: compile
	@if [ -f $(PLT) ]; then \
		dialyzer --check_plt --plt $(PLT) --apps $(DIALYZER_APPS) && \
		dialyzer --add_to_plt --plt $(PLT) --output_plt $(PLT) --apps $(DIALYZER_APPS) ; test $$? -ne 1; \
	else \
		dialyzer --build_plt --output_plt $(PLT) --apps $(DIALYZER_APPS); test $$? -ne 1; \
	fi

${LOCAL_PLT}: compile
	@if [ -d deps ]; then \
		if [ -f $(LOCAL_PLT) ]; then \
			dialyzer --check_plt --plt $(LOCAL_PLT) deps/*/ebin  && \
			dialyzer --add_to_plt --plt $(LOCAL_PLT) --output_plt $(LOCAL_PLT) deps/*/ebin ; test $$? -ne 1; \
		else \
			dialyzer --build_plt --output_plt $(LOCAL_PLT) deps/*/ebin ; test $$? -ne 1; \
		fi \
	fi
		
dialyzer: ${PLT} ${LOCAL_PLT}
	@echo "==> $(shell basename $(shell pwd)) (dialyzer)"
	@if [ -f $(LOCAL_PLT) ]; then \
		PLTS="$(PLT) $(LOCAL_PLT)"; \
	else \
		PLTS=$(PLT); \
	fi; \
	if [ -f dialyzer_expected ]; then \
		dialyzer $(DIALYZER_FLAGS) --plts $${PLTS} -c ebin | tee dialyzer_warnings && \
		diff -U0 dialyzer_expected dialyzer_warnings; \
	else \
		dialyzer $(DIALYZER_FLAGS) --plts $${PLTS} -c ebin; \
	fi

cleanplt:
	@echo 
	@echo "Are you sure?  It takes several minutes to re-build."
	@echo Deleting $(PLT) and $(LOCAL_PLT) in 5 seconds.
	@echo 
	sleep 5
	rm $(PLT)
	rm $(LOCAL_PLT)

