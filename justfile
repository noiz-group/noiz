timestamp := `date +%s`
env_file_system_test := "env_system_tests"

_default:
    just --list

clean_after_tests:
    rm -rf system_test_processed_data_dir_*

prepare_dotenv:
    #! /usr/bin/env bash
    echo NOIZ_POSTGRES_HOST=localhost >> {{env_file_system_test}}
    echo NOIZ_POSTGRES_PORT="5432" >> {{env_file_system_test}}
    echo NOIZ_POSTGRES_USER=noiztest >> {{env_file_system_test}}
    echo NOIZ_POSTGRES_PASSWORD=noiztest >> {{env_file_system_test}}
    echo NOIZ_POSTGRES_DB=noiztest_{{timestamp}} >> {{env_file_system_test}}
    echo NOIZ_MSEEDINDEX_EXECUTABLE=../mseedindex/mseedindex >> {{env_file_system_test}}
    echo SQLALCHEMY_WARN_20=1 >> {{env_file_system_test}}

    export NOIZ_PROCESSED_DATA_DIR="system_test_processed_data_dir_{{timestamp}}/"
    echo NOIZ_PROCESSED_DATA_DIR=$NOIZ_PROCESSED_DATA_DIR >> {{env_file_system_test}}

    mkdir $NOIZ_PROCESSED_DATA_DIR

run_system_tests: prepare_dotenv
    #! /usr/bin/env bash
    export $(grep -v '^#' {{env_file_system_test}} | xargs)

    python tests/system_tests/create_new_db.py

    python -m noiz db migrate
    python -m noiz db upgrade
    python -m pytest --runcli

    rm {{env_file_system_test}}

submodule cmd="":
	#! /bin/bash
	set -euf -o pipefail
	if [[ "{{cmd}}" == "pull" ]]
	then
		git submodule foreach git pull origin main
	elif [[ "{{cmd}}" == "update" ]]
	then
		git submodule init
		git submodule update
	else
		echo "The command {{cmd}} does not exist."
	fi
