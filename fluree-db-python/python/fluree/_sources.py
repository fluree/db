"""Graph sources: tables in Iceberg, Delta Lake or a SQL engine, mapped to RDF
by an R2RML mapping and queried in place, without copying them into a
ledger."""

from __future__ import annotations

import os
import warnings
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any, Union

from fluree.errors import InvalidRequestError

if TYPE_CHECKING:
    from fluree._connection import Connection, Language, Query, QueryProfile, SelectLanguage
    from fluree._results import Result


@dataclass(frozen=True, slots=True)
class EnvVar:
    """A secret read from environment variable ``name`` where the source is
    read, so the secret itself is never stored with the graph source."""

    name: str


Secret = Union[str, EnvVar]


def _secret(value: Secret) -> Any:
    if isinstance(value, EnvVar):
        return {"env_var": value.name}
    if isinstance(value, str):
        return value
    raise TypeError(f"a secret is a str or an EnvVar, not {type(value).__name__}")


@dataclass(frozen=True, slots=True)
class Bearer:
    """Authenticate with a static bearer token."""

    token: Secret

    def _json(self) -> dict[str, Any]:
        return {"type": "bearer", "token": _secret(self.token)}


@dataclass(frozen=True, slots=True)
class OAuth2:
    """Authenticate with OAuth2 client credentials, fetching (and refreshing)
    a token from ``token_url`` (which a :class:`Unity` catalog can leave to
    its default)."""

    token_url: str | None
    client_id: str
    client_secret: Secret
    scope: str | None = None
    audience: str | None = None

    def _json(self) -> dict[str, Any]:
        return {
            "type": "oauth2_client_credentials",
            "token_url": self.token_url,
            "client_id": self.client_id,
            "client_secret": _secret(self.client_secret),
            "scope": self.scope,
            "audience": self.audience,
        }


Auth = Union[Bearer, OAuth2]


def _auth(auth: Auth | None) -> dict[str, Any] | None:
    if auth is None:
        return None
    if not isinstance(auth, (Bearer, OAuth2)):
        raise TypeError(f"auth is a fluree.Bearer or fluree.OAuth2, not {type(auth).__name__}")
    if isinstance(auth, OAuth2) and not auth.token_url:
        raise InvalidRequestError("OAuth2 needs a token_url here")
    return auth._json()


@dataclass(frozen=True, slots=True)
class Unity:
    """A Databricks Unity Catalog that Delta tables are found and read
    through. ``catalog`` and ``schema`` complete table names of fewer than
    three parts. An :class:`OAuth2` ``token_url`` defaults to the
    workspace's own endpoint."""

    uri: str
    catalog: str | None = None
    schema: str | None = None
    auth: Auth | None = None

    def _json(self) -> dict[str, Any]:
        spec: dict[str, Any] = {
            "uri": self.uri,
            "catalog": self.catalog,
            "schema": self.schema,
            "bearer": None,
            "client_id": None,
            "client_secret": None,
            "token_url": None,
            "scope": None,
        }
        if isinstance(self.auth, Bearer):
            spec["bearer"] = _secret(self.auth.token)
        elif isinstance(self.auth, OAuth2):
            spec["client_id"] = self.auth.client_id
            spec["client_secret"] = _secret(self.auth.client_secret)
            spec["token_url"] = self.auth.token_url
            spec["scope"] = self.auth.scope
        elif self.auth is not None:
            raise TypeError(f"auth is a fluree.Bearer or fluree.OAuth2, not {type(self.auth).__name__}")
        return spec


@dataclass(frozen=True, slots=True)
class AzureServicePrincipal:
    """Microsoft Entra service-principal credentials for Delta tables in
    ADLS Gen2 or OneLake."""

    tenant_id: str
    client_id: str
    client_secret: Secret

    def _json(self) -> dict[str, Any]:
        secret = self.client_secret
        return {
            "tenant_id": self.tenant_id,
            "client_id": self.client_id,
            "client_secret": secret if isinstance(secret, str) else None,
            "client_secret_env": secret.name if isinstance(secret, EnvVar) else None,
        }


@dataclass(frozen=True, slots=True)
class MaterializeResult:
    """One :meth:`GraphSource.materialize` pass.

    ``snapshot`` is the source snapshot now reflected in the ledger (``None``
    for a table with no snapshots yet). ``incremental`` says whether only
    what the source added since the last pass was read; ``committed`` whether
    anything changed in the ledger."""

    snapshot: int | None
    incremental: bool
    committed: bool
    rows_read: int
    subjects_upserted: int
    subjects_retracted: int


class GraphSource:
    """A graph source: tables mapped to RDF and queried in place.

    Create one with :meth:`Connection.map_iceberg`, :meth:`Connection.map_delta`
    or :meth:`Connection.map_sql`, or look one up with
    :meth:`Connection.graph_source`. Query it on its own here, or with other
    graph sources and ledgers through :meth:`Connection.query`, naming it in
    ``FROM <name:main>`` (SPARQL) or ``"from"`` (JSON-LD).
    """

    __slots__ = ("_connection", "branch", "id", "kind", "name")

    def __init__(self, connection: Connection, raw: Mapping[str, Any]) -> None:
        self._connection = connection
        self.id: str = raw["id"]
        self.name: str = raw["name"]
        self.branch: str = raw["branch"]
        self.kind: str = raw["kind"]

    def __repr__(self) -> str:
        return f"<GraphSource {self.id!r} {self.kind}>"

    def __eq__(self, other: object) -> bool:
        return isinstance(other, GraphSource) and other.id == self.id

    def __hash__(self) -> int:
        return hash(self.id)

    def query(
        self,
        query: Query,
        parameters: Mapping[str, Any] | None = None,
        *,
        language: Language | None = None,
        max_fuel: float | None = None,
        timeout: float | None = None,
        **kwparameters: Any,
    ) -> Any:
        """Run a SPARQL or JSON-LD query of this source; see
        :meth:`Snapshot.query`. Cypher reads ledgers only."""
        from fluree._connection import _execute
        from fluree._params import _params

        return _execute(
            self._run, query, max_fuel, timeout, False,
            language=language, params=_params(parameters, kwparameters),
        )

    def select(
        self,
        query: str,
        parameters: Mapping[str, Any] | None = None,
        *,
        language: SelectLanguage | None = None,
        max_fuel: float | None = None,
        timeout: float | None = None,
        **kwparameters: Any,
    ) -> Result:
        """Run a SPARQL ``SELECT`` of this source; see :meth:`Snapshot.select`."""
        from fluree._connection import _execute
        from fluree._params import _params

        return _execute(
            self._run, query, max_fuel, timeout, False,
            language=language, params=_params(parameters, kwparameters), select=True,
        )

    def profile(
        self,
        query: Query,
        parameters: Mapping[str, Any] | None = None,
        *,
        language: Language | None = None,
        max_fuel: float | None = None,
        timeout: float | None = None,
        **kwparameters: Any,
    ) -> QueryProfile:
        """Run :meth:`query` and report the fuel and time it took."""
        from fluree._connection import _profile
        from fluree._params import _params

        return _profile(
            self._run, query, max_fuel, timeout,
            language=language, params=_params(parameters, kwparameters),
        )

    def materialize(self, into: str, *, full: bool = False) -> MaterializeResult:
        """Copy this source's rows into ledger ``into`` (created if missing)
        as ordinary ledger data, so they gain history, policy and indexes.

        Each pass reads only what the source has added since the last pass
        into ``into``, where the table's history allows; ``full`` re-reads
        everything. A row whose subject reappears replaces it. Iceberg
        sources only."""
        native = self._connection._native
        return MaterializeResult(**native.materialize(self.id, into, full))

    def drop(self) -> None:
        """Remove this graph source. The tables themselves are not touched."""
        self._connection._native.drop_graph_source(self.name, self.branch)

    def _run(self, query: Any, language: str, controls: dict[str, Any] | None, params: Any) -> Any:
        if language == "cypher":
            raise InvalidRequestError("Cypher reads ledgers; query a graph source with SPARQL or JSON-LD")
        return self._connection._native.query_graph(self.id, query, controls, params)


def _mapping(mapping: str | os.PathLike[str]) -> str:
    """R2RML mapping text, given as Turtle or as the path of a Turtle file."""
    if isinstance(mapping, os.PathLike):
        return Path(mapping).read_text(encoding="utf-8")
    if isinstance(mapping, str):
        return mapping
    raise TypeError(f"a mapping is R2RML Turtle text or a path, not {type(mapping).__name__}")


def _common(
    name: str,
    mapping: str | os.PathLike[str],
    branch: str | None,
    model: str | None,
    default_allow: bool | None,
) -> dict[str, Any]:
    return {
        "name": name,
        "branch": branch,
        "mapping": _mapping(mapping),
        "mapping_type": "text/turtle",
        "model": model,
        "default_allow": default_allow,
    }


def _registered(connection: Connection, raw: Mapping[str, Any]) -> GraphSource:
    for warning in raw["warnings"]:
        warnings.warn(warning, stacklevel=3)
    return connection.graph_source(raw["id"])
