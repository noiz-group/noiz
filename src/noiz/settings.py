# SPDX-License-Identifier: CECILL-B
# Copyright © 2015-2019 EOST UNISTRA, Storengy SAS, Damian Kula
# Copyright © 2019-2023 Contributors to the Noiz project.
import warnings
from typing import Optional

from pydantic import Field, PostgresDsn
from pydantic_settings import BaseSettings, SettingsConfigDict

from noiz.exceptions import ValidationError, ConfigError
from noiz.globals import ExtendedEnum


class SupportedDbs(str, ExtendedEnum):
    postgres = "postgres"


class NoizSettings(BaseSettings):
    database: Optional[SupportedDbs] = SupportedDbs.postgres
    PG_DSN: Optional[PostgresDsn] = None

    POSTGRES_HOST: Optional[str] = None
    postgres_host_deprecated: Optional[str] = Field(alias="postgres_host", default=None)
    POSTGRES_PORT: Optional[int] = None
    postgres_port_deprecated: Optional[int] = Field(alias="postgres_port", default=None)
    POSTGRES_USER: Optional[str] = None
    postgres_user_deprecated: Optional[str] = Field(alias="postgres_user", default=None)
    POSTGRES_PASSWORD: Optional[str] = None
    postgres_password_deprecated: Optional[str] = Field(alias="postgres_password", default=None)
    POSTGRES_DB: Optional[str] = None
    postgres_db_deprecated: Optional[str] = Field(alias="postgres_db", default=None)

    SQLALCHEMY_DATABASE_URI: Optional[str] = None
    sqlalchemy_database_uri_deprecated: Optional[str] = Field(alias="sqlalchemy_database_uri", default=None)

    FLASK_ENV: Optional[str] = "development"
    flask_env_deprecated: Optional[str] = Field(alias="flask_env", default=None)

    PROCESSED_DATA_DIR: Optional[str] = None
    processed_data_dir_deprecated: Optional[str] = Field(alias="processed_data_dir", default=None)

    MSEEDINDEX_EXECUTABLE: Optional[str] = None
    mseedindex_executable_deprecated: Optional[str] = Field(alias="mseedindex_executable", default=None)

    SQLALCHEMY_TRACK_MODIFICATIONS: bool = False
    DEBUG: bool = False  # No idea if this is needed by flask

    model_config = SettingsConfigDict(env_prefix='noiz_')

    def model_post_init(self, __context):
        if self.database == SupportedDbs.postgres:
            self.PG_DSN = self.validate_postgres_dsn()
            self.SQLALCHEMY_DATABASE_URI = self.PG_DSN.unicode_string()
        else:
            raise ConfigError(f"Database {self.database} is not supported. Please select one of {SupportedDbs.list()}")

        # if self.sqlalchemy_database_uri_deprecated and not self.sqlalchemy_database_uri:
        #     warnings.warn(f"Variable {'sqlalchemy_database_uri'.upper()} is deprecated. "
        #                   f"Please use NOIZ_{'sqlalchemy_database_uri'.upper()} instead")
        # self.SQLALCHEMY_DATABASE_URI = f"postgresql+psycopg2://{self.POSTGRES_USER}:{self.POSTGRES_PASSWORD}@" \
        #                                f"{self.POSTGRES_HOST}:{self.POSTGRES_PORT}/{self.POSTGRES_DB}"

        self.FLASK_ENV = self._check_deprecated_pair(
            value=self.FLASK_ENV,
            value_deprecated=self.flask_env_deprecated,
            name=f"{self.model_config.get('env_prefix')}flask_env".upper(),
            name_deprecated="flask_env".upper(),
        )

        self.PROCESSED_DATA_DIR = self._check_deprecated_pair(
            value=self.PROCESSED_DATA_DIR,
            value_deprecated=self.processed_data_dir_deprecated,
            name=f"{self.model_config.get('env_prefix')}processed_data_dir".upper(),
            name_deprecated="processed_data_dir".upper(),
        )

        self.MSEEDINDEX_EXECUTABLE = self._check_deprecated_pair(
            value=self.MSEEDINDEX_EXECUTABLE,
            value_deprecated=self.mseedindex_executable_deprecated,
            name=f"{self.model_config.get('env_prefix')}mseedindex_executable".upper(),
            name_deprecated="mseedindex_executable".upper(),
        )

        if self.FLASK_ENV == "development":
            self.DEBUG = True
        else:
            self.DEBUG = False

    def validate_postgres_dsn(self) -> PostgresDsn:
        self.POSTGRES_HOST = self._check_deprecated_pair(
            value=self.POSTGRES_HOST,
            value_deprecated=self.postgres_host_deprecated,
            name=f"{self.model_config.get('env_prefix')}postgres_host".upper(),
            name_deprecated="postgres_host".upper(),
        )
        self.POSTGRES_PORT = self._check_deprecated_pair(
            value=self.POSTGRES_PORT,
            value_deprecated=self.postgres_port_deprecated,
            name=f"{self.model_config.get('env_prefix')}postgres_port".upper(),
            name_deprecated="postgres_port".upper(),
        )
        self.POSTGRES_USER = self._check_deprecated_pair(
            value=self.POSTGRES_USER,
            value_deprecated=self.postgres_user_deprecated,
            name=f"{self.model_config.get('env_prefix')}postgres_user".upper(),
            name_deprecated="postgres_user".upper(),
        )
        self.POSTGRES_PASSWORD = self._check_deprecated_pair(
            value=self.POSTGRES_PASSWORD,
            value_deprecated=self.postgres_password_deprecated,
            name=f"{self.model_config.get('env_prefix')}postgres_password".upper(),
            name_deprecated="postgres_password".upper(),
        )
        self.POSTGRES_DB = self._check_deprecated_pair(
            value=self.POSTGRES_DB,
            value_deprecated=self.postgres_db_deprecated,
            name=f"{self.model_config.get('env_prefix')}postgres_db".upper(),
            name_deprecated="postgres_db".upper(),
        )

        db_uri = f"postgresql+psycopg2://{self.POSTGRES_USER}:{self.POSTGRES_PASSWORD}@" \
                 f"{self.POSTGRES_HOST}:{self.POSTGRES_PORT}/{self.POSTGRES_DB}"

        return PostgresDsn(db_uri)

    @staticmethod
    def _check_deprecated_pair(
            value,
            value_deprecated,
            name: str,
            name_deprecated: str,
    ):
        if value is None and value_deprecated is None:
            raise ValidationError(f"Missing value of {name_deprecated} config param."
                                  f"Please set environment variable: {name}")
        if value_deprecated and not value:
            warnings.warn(f"Environment variable {name_deprecated} is deprecated! "
                          f"Please use variable: {name}")
            return value_deprecated
        return value
