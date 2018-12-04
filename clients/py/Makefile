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
	rm -r dist
	python setup.py sdist bdist_wheel
	pipenv run twine upload dist/*

.PHONY: clean_pyc
clean_pyc:
	find . -name '*.pyc' -exec rm {} \;

.PHONY: flask8
flake8:
	PIPENV_IGNORE_VIRTUALENVS=1 \
	    pipenv run flake8 \
	    	--exclude 'frames_pb2*.py' \
	    	v3io_frames tests

.PHONY: test
test: clean_pyc flake8
	PIPENV_IGNORE_VIRTUALENVS=1 \
	    pipenv run python -m pytest -v \
		--disable-warnings \
		--benchmark-disable \
		tests

README.html: README.md
	kramdown -i GFM $< > $@

.PHONY: update-deps
update-deps:
	PIPENV_IGNORE_VIRTUALENVS=1 \
	    pipenv update

.PHONY sync-deps
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
