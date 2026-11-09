"""Project settings, read from environment variables and the .env file."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

# Look for .env in the project root, whatever the current directory is.
PROJECT_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(PROJECT_ROOT / ".env")


def _path(value: str) -> str:
    """Resolve relative paths against the project root."""
    p = Path(value)
    return str(p if p.is_absolute() else PROJECT_ROOT / p)


@dataclass(frozen=True)
class Settings:
    sink: str
    duckdb_path: str
    landing_dir: str
    archive_dir: str
    dead_letter_dir: str
    kafka_bootstrap: str
    kafka_topic: str
    kafka_group_id: str
    kafka_security_protocol: str
    kafka_ssl_ca: str
    kafka_ssl_cert: str
    kafka_ssl_key: str
    kafka_sasl_username: str
    kafka_sasl_password: str


def get_settings() -> Settings:
    e = os.getenv
    return Settings(
        sink=e("SINK", "jsonl"),
        duckdb_path=_path(e("DUCKDB_PATH", "data/warehouse.duckdb")),
        landing_dir=_path(e("LANDING_DIR", "data/landing")),
        archive_dir=_path(e("ARCHIVE_DIR", "data/archive")),
        dead_letter_dir=_path(e("DEAD_LETTER_DIR", "data/dead_letter")),
        kafka_bootstrap=e("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        kafka_topic=e("KAFKA_TOPIC", "llm_usage"),
        kafka_group_id=e("KAFKA_GROUP_ID", "llm-cost-ingestion"),
        kafka_security_protocol=e("KAFKA_SECURITY_PROTOCOL", "SSL").upper(),
        kafka_ssl_ca=_path(e("KAFKA_SSL_CA", "certs/ca.pem")),
        kafka_ssl_cert=_path(e("KAFKA_SSL_CERT", "certs/service.cert")),
        kafka_ssl_key=_path(e("KAFKA_SSL_KEY", "certs/service.key")),
        kafka_sasl_username=e("KAFKA_SASL_USERNAME", ""),
        kafka_sasl_password=e("KAFKA_SASL_PASSWORD", ""),
    )


def kafka_config(s: Settings) -> dict[str, str]:
    """librdkafka connection settings for the chosen security protocol.

    * SSL        – Aiven default: client certificate authentication (3 files)
    * SASL_SSL   – username/password over TLS (Aiven's SASL option)
    * PLAINTEXT  – local/CI brokers without auth
    """
    conf: dict[str, str] = {
        "bootstrap.servers": s.kafka_bootstrap,
        "security.protocol": s.kafka_security_protocol,
        "client.id": "llm-cost-observatory",
    }
    proto = s.kafka_security_protocol
    if proto in ("SSL", "SASL_SSL"):
        _require_file(s.kafka_ssl_ca, "KAFKA_SSL_CA (CA certificate)")
        conf["ssl.ca.location"] = s.kafka_ssl_ca
    if proto == "SSL":
        _require_file(s.kafka_ssl_cert, "KAFKA_SSL_CERT (access certificate)")
        _require_file(s.kafka_ssl_key, "KAFKA_SSL_KEY (access key)")
        conf["ssl.certificate.location"] = s.kafka_ssl_cert
        conf["ssl.key.location"] = s.kafka_ssl_key
    if proto == "SASL_SSL":
        conf.update({
            "sasl.mechanism": "SCRAM-SHA-256",
            "sasl.username": s.kafka_sasl_username,
            "sasl.password": s.kafka_sasl_password,
        })
    return conf


def _require_file(path: str, label: str) -> None:
    if not Path(path).is_file():
        raise FileNotFoundError(
            f"{label} not found at '{path}'. Download it from the Aiven console into the certs/ folder "
            f"(see docs/KAFKA_SETUP.md), or set the path in .env."
        )
