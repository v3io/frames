# Copyright 2018 Iguazio
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

.PHONY: all
all:
	$(error please pick a target)

.PHONY: upload
upload:
	pipenv run python pypi_upload.py --force

.PHONY: clean_pyc
clean_pyc:
	find . -name '*.pyc' -exec rm {} \;

.PHONY: flake8
flake8:
	# --ignore=E121,E123,E126,E226,E24,E704 is the default.
	# Additionally, E501 ignores long lines.
	PIPENV_IGNORE_VIRTUALENVS=1 \
	    FRAMES_PYTHON_DEBUG=true pipenv run flake8 \
	        --ignore=E121,E123,E126,E226,E24,E704,E501 \
	        --exclude 'frames_pb2*.py' \
	        v3io_frames tests

.PHONY: test
test: clean_pyc flake8
	PIPENV_IGNORE_VIRTUALENVS=1 \
	    FRAMES_PYTHON_DEBUG=true pipenv run python -m pytest -v \
		--disable-warnings \
		--benchmark-disable \
		tests

README.html: README.md
	kramdown -i GFM $< > $@

.PHONY: update-deps
update-deps:
	PIPENV_IGNORE_VIRTUALENVS=1 \
	    pipenv update

.PHONY: sync-deps
sync-deps:
	PIPENV_IGNORE_VIRTUALENVS=1 \
	    pipenv sync --dev

.PHONY: bench
bench:
	PIPENV_IGNORE_VIRTUALENVS=1 \
	    pipenv run python -m pytest \
		--disable-warnings \
		--benchmark-json /tmp/framesd-py-bench.json \
		tests/test_benchmark.py
