.PHONY: all
all: package

include ../../common.mk
include ../lambdas.mk

.PHONY: wheels
wheels: check_env
	rm -rf vendor/
	for wheel in $(project_root)/lambdas/.wheels/*.whl; do unzip $$wheel -d vendor; done

.PHONY: package
package: check_branch check_python check_aws config wheels
	python $(project_root)/scripts/stage_layer.py
