"""Graph sources: tables in Iceberg, Delta Lake or a SQL engine, mapped to RDF
by an R2RML mapping and queried in place, without copying them into a
ledger."""

from __future__ import annotations

import json
import os
import warnings
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any, Union
from typing import Literal as _Literal

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


@dataclass(frozen=True, slots=True)
class Google:
    """Authenticate as a Google identity, its tokens minted and refreshed
    automatically: for a Google Iceberg REST catalog (BigLake), a SQL
    endpoint, or (as ``storage``) the ``gs://`` files of a source.

    ``credentials`` says where the identity comes from:

    - ``"metadata"`` (the default): the GCE/GKE metadata server's service
      account;
    - ``"application_default"``: the host's application-default credentials
      (``GOOGLE_APPLICATION_CREDENTIALS``, else gcloud's), e.g. after
      ``gcloud auth application-default login``. A user's credentials carry
      the user's own cloud-platform scope, which a refresh cannot narrow to
      read-only storage, so use them for development;
    - an :class:`EnvVar` naming the variable that holds a service-account
      key's JSON, read where the source is read. A key is never given
      literally: it would be stored with the source;
    - a Workload Identity Federation credential configuration, what ``gcloud
      iam workload-identity-pools create-cred-config ... --aws`` prints: as a
      dict, or the path of its file. It is not a secret, and is stored.

    The metadata server and application-default credentials are the host's
    own identity, which an operator can refuse
    (``FLUREE_GRAPH_SOURCE_HOST_IDENTITIES``). ``quota_project`` is the
    project requests bill to (``x-goog-user-project``). BigLake requires one
    for every identity; as a catalog's auth it defaults to the project a
    ``bq://projects/<P>/...`` or ``bl://projects/<P>/...`` warehouse names,
    and it bills the catalog only, never storage reads. As ``storage`` it is
    the requester-pays billing project, never defaulted. ``scopes`` is a
    comma-separated scope list (default cloud-platform, catalog auth only)."""

    credentials: Union[str, EnvVar, Mapping[str, Any], os.PathLike[str]] = "metadata"
    quota_project: str | None = None
    scopes: str | None = None

    def _credentials(self) -> dict[str, Any]:
        credentials = self.credentials
        if isinstance(credentials, EnvVar):
            return {"source": "service_account", "key": {"env_var": credentials.name}}
        if isinstance(credentials, os.PathLike):
            return _wif_file(Path(credentials), os.fspath(credentials))
        if isinstance(credentials, Mapping):
            return {"source": "external_account", "config": dict(credentials)}
        if credentials == "metadata":
            return {"source": "metadata"}
        if credentials == "application_default":
            return {"source": "application_default"}
        if isinstance(credentials, str):
            # Any other string is the path of a WIF configuration file, unless
            # it is JSON text: a key given literally. Neither is repeated in a
            # refusal, since the string may be a key.
            if not credentials.lstrip().startswith("{") and Path(credentials).is_file():
                return _wif_file(Path(credentials), "the file given")
            raise InvalidRequestError(
                'Google credentials are "metadata", "application_default", an EnvVar holding a '
                "service-account key (a key is never given literally, since it would be stored "
                "with the source), or a Workload Identity Federation configuration (a dict, or "
                "the path of its file; no file is at the path given)"
            )
        raise TypeError(
            "Google credentials are a str, an EnvVar, a dict or a path, not "
            f"{type(credentials).__name__}"
        )

    def _json(self) -> dict[str, Any]:
        spec: dict[str, Any] = {"type": "google", "credentials": self._credentials()}
        if self.scopes is not None:
            spec["scopes"] = self.scopes
        if self.quota_project is not None:
            spec["quota_project"] = self.quota_project
        return spec

    def _storage_json(self) -> dict[str, Any]:
        if self.scopes is not None:
            raise InvalidRequestError(
                "a storage identity reads with the storage scope; scopes apply to catalog auth"
            )
        spec: dict[str, Any] = {"type": "google", "credentials": self._credentials()}
        if self.quota_project is not None:
            spec["quota_project"] = self.quota_project
        return spec


def _wif_file(path: Path, named: str) -> dict[str, Any]:
    """A Workload Identity Federation credential configuration from its file."""
    try:
        text = path.read_text(encoding="utf-8")
    except OSError as e:
        raise InvalidRequestError(
            f"cannot read the Workload Identity Federation credential configuration {named}: "
            f"{e.strerror or type(e).__name__}"
        ) from None
    try:
        config = json.loads(text)
    except ValueError:
        config = None
    if not isinstance(config, dict):
        raise InvalidRequestError(
            f"{named} does not hold a Workload Identity Federation credential configuration "
            "(a JSON object)"
        )
    return {"source": "external_account", "config": config}


Auth = Union[Bearer, OAuth2, Google]


def _auth(auth: Auth | None) -> dict[str, Any] | None:
    if auth is None:
        return None
    if not isinstance(auth, (Bearer, OAuth2, Google)):
        raise TypeError(
            f"auth is a fluree.Bearer, fluree.OAuth2 or fluree.Google, not {type(auth).__name__}"
        )
    if isinstance(auth, OAuth2) and not auth.token_url:
        raise InvalidRequestError("OAuth2 needs a token_url here")
    return auth._json()


StorageIdentity = Union[_Literal["vended", "ambient", "catalog"], Google]


def _storage(storage: StorageIdentity | None) -> dict[str, Any] | None:
    """Which identity reads a source's files, in its stored shape."""
    if storage is None:
        return None
    if isinstance(storage, Google):
        return storage._storage_json()
    if storage in ("vended", "ambient", "catalog"):
        return {"type": storage}
    raise InvalidRequestError(
        f'storage is "vended", "ambient", "catalog" or a fluree.Google, not {storage!r}'
    )


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
class Glue:
    """An AWS Glue Data Catalog that Iceberg tables are found through: each
    mapped ``rr:tableName`` is a Glue ``<database>.<table>``, whose current
    metadata file Glue names. The catalog call and the S3 reads use the usual
    AWS environment, never vended credentials. ``region`` is the catalog's
    region (else ``s3_region``, else the AWS region chain); ``catalog_id``
    reads another account's catalog."""

    region: str | None = None
    catalog_id: str | None = None

    def _json(self) -> dict[str, Any]:
        return {"region": self.region, "catalog_id": self.catalog_id}


@dataclass(frozen=True, slots=True)
class S3Tables:
    """An AWS S3 Tables table bucket that Iceberg tables are found in, named by
    its ARN (``arn:aws:s3tables:<region>:<account>:bucket/<name>``): each
    mapped ``rr:tableName`` is a ``<namespace>.<table>`` in the bucket. The
    region defaults to the ARN's. Reads use the usual AWS environment."""

    table_bucket_arn: str
    region: str | None = None

    def _json(self) -> dict[str, Any]:
        return {"table_bucket_arn": self.table_bucket_arn, "region": self.region}


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

    ``id`` is ``name:branch``, and ``kind`` the kind of source, such as
    ``"iceberg"``, ``"delta"`` or ``"sql"``.
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
