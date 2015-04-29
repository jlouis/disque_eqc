compile:
	rebar3 compile | sed -e 's|/_build/default/lib/disque_eqc||g'
