//! The skip registers: each entry is a test that fails today, grouped by its
//! cause, which is a documented deviation or an open issue. `check_suite`
//! fails on a registered test that passes, so an entry leaves in the change
//! that fixes it. Baseline: json-ld-api @ 5551473.

pub const EXPAND: &[&str] = &[
    // A remote context is not fetched: there is no document loader, and a string
    // `@context` sets `@vocab` to the string. Documented: compatibility.md (JSON-LD 1.1).
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#t0126", // A scoped context may include itself recursively (direct)
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#t0127", // A scoped context may include itself recursively (indirect)
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#t0128", // Two scoped context may include a shared context
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#tc031", // @context resolutions respects relative URLs.
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#tc034", // Remote scoped context.
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#ter04", // Error dereferencing a remote context
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#ter05", // Invalid remote context
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#tso02", // @import must be a string
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#tso03", // @import overflow
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#tso05", // @propagate: true on type-scoped context with @import
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#tso06", // @propagate: false on property-scoped context with @import
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#tso07", // Protect all terms in sourced context
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#tso10", // Protect terms in sourced context
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#tso12", // @import may not be used in an imported context.
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#tso13", // @import can only reference a single context
    // `@container` `@id`, `@type` and `@graph`, and a property-valued `@index`, are
    // refused. Documented: compatibility.md (JSON-LD 1.1).
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#t0079", // expand @graph container
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#t0080", // expand [@graph, @set] container
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#t0081", // Creates an @graph container if value is a graph
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#t0082", // expand [@graph, @index] container
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#t0083", // expand [@graph, @index, @set] container
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#t0084", // Do not expand [@graph, @index] container if value is a graph
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#t0085", // expand [@graph, @id] container
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#t0086", // expand [@graph, @id, @set] container
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#t0087", // Do not expand [@graph, @id] container if value is a graph
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#t0093", // expand @graph container (multiple objects)
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#t0094", // expand [@graph, @set] container (multiple objects)
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#t0095", // Creates an @graph container if value is a graph (multiple objects)
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#t0096", // expand [@graph, @index] container (multiple indexed objects)
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#t0097", // expand [@graph, @index, @set] container (multiple objects)
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#t0098", // Do not expand [@graph, @index] container if value is a graph (multiple objects)
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#t0099", // expand [@graph, @id] container (multiple objects)
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#t0100", // expand [@graph, @id, @set] container (multiple objects)
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#t0101", // Do not expand [@graph, @id] container if value is a graph (multiple objects)
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#t0102", // Expand @graph container if value is a graph (multiple objects)
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#t0103", // Expand @graph container if value is a graph (multiple graphs)
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#t0104", // Creates an @graph container if value is a graph (mixed graph and object)
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#t0105", // Do not expand [@graph, @index] container if value is a graph (mixed graph and object)
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#t0106", // Do not expand [@graph, @id] container if value is a graph (mixed graph and object)
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#t0107", // expand [@graph, @index] container (indexes with multiple objects)
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#t0108", // expand [@graph, @id] container (multiple ids and objects)
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#t0131", // Reverse term with property based indexed container
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#tc013", // type maps use scoped context from type index and not scoped context from containing
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#tc025", // type-scoped + graph container
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#tm001", // Adds @id to object not having an @id
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#tm002", // Retains @id in object already having an @id
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#tm003", // Adds @type to object not having an @type
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#tm004", // Prepends @type in object already having an @type
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#tm005", // Adds expanded @id to object
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#tm006", // Adds vocabulary expanded @type to object
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#tm007", // Adds document expanded @type to object
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#tm008", // When type is in a type map
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#tm011", // id map with @none
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#tm012", // type map with alias of @none
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#tm013", // graph index map with @none
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#tm014", // graph index map with alias @none
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#tm015", // graph id index map with aliased @none
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#tm016", // graph id index map with aliased @none
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#tm017", // string value of type map expands to node reference
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#tm018", // string value of type map expands to node reference with @type: @id
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#tm019", // string value of type map expands to node reference with @type: @vocab
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#tpi04", // error if @index is not a string for property-valued index
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#tpi06", // property-valued index expands to property value, instead of @index (value)
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#tpi07", // property-valued index appends to property value, instead of @index (value)
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#tpi08", // property-valued index expands to property value, instead of @index (node)
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#tpi09", // property-valued index appends to property value, instead of @index (node)
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#tpi10", // property-valued index does not output property for @none
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#tpi11", // property-valued index adds property to graph object
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#tpr25", // Allows redefinition of terms with scoped contexts using same definitions.
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#tpr43", // Clear protection in @graph @container with null context.
    // `@index` is accepted and ignored, so the expanded output has none.
    // Documented: compatibility.md (JSON-LD 1.1).
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#t0036", // Expanding @index
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#t0044", // Index maps with language mappings
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#t0063", // Reverse property and index container
    // A key in keyword form that is no keyword (`"@ignoreMe"`) is kept on the node,
    // where the algorithm drops it, and a transaction refuses it. Documented:
    // compatibility.md (JSON-LD 1.1).
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#t0119", // Ignore some terms with @, allow others.
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#t0120", // Ignore some values of @id with @, allow others.
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#t0121", // Ignore some values of @reverse with @, allow others.
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#t0122", // Ignore some IRIs when that start with @ when expanding.
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#tpr34", // Ignores a non-keyword term starting with '@'
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#tpr35", // Ignores a non-keyword term starting with '@' (with @vocab)
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#tpr36", // Ignores a term mapping to a value in the form of a keyword.
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#tpr37", // Ignores a term mapping to a value in the form of a keyword (with @vocab).
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#tpr38", // Ignores a term mapping to a value in the form of a keyword (@reverse).
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#tpr39", // Ignores a term mapping to a value in the form of a keyword (@reverse with @vocab).
    // A node with several types applies their scoped contexts in the order
    // written, and `@propagate` is honored on a type's scoped context only.
    // Documented: compatibility.md (JSON-LD 1.1); #2080.
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#tc011", // orders @type terms when applying scoped contexts
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#tc017", // multiple type-scoped contexts are properly reverted
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#tc018", // multiple type-scoped types resolved against previous context
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#tc027", // @propagate: false on property-scoped context
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#tc028", // @propagate: false on embedded context
    // A string under a `@json` term expands to its serialized form, since a string
    // `@value` typed `@json` is read as serialized JSON. Documented: compatibility.md (JSON-LD 1.1).
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#tjs17", // Expand JSON literal (string)
    // Properties beside a top-level `@graph`, with no `@id`, are transaction
    // metadata. Documented: docs/transactions/overview.md.
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#t0021", // do not remove @graph at top-level if not only property
    // A `@direction` in a `@context`, as the default or on a term, is not read.
    // Documented: compatibility.md (base-direction tags); #2360.
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#tdi01", // Expand string using default and term directions
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#tdi02", // Expand string using default and term directions and languages
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#tdi03", // expand list values with @direction
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#tdi04", // simple language map with term direction
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#tdi05", // simple language mapwith overriding term direction
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#tdi06", // simple language mapwith overriding null direction
    // There is no JSON-LD 1.0 processing mode (`processingMode: json-ld-1.0`): a
    // document is processed as JSON-LD 1.1. Documented: compatibility.md (JSON-LD 1.1).
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#t0075", // @vocab as blank node identifier
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#tc029", // @propagate is invalid in 1.0
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#tep02", // processingMode json-ld-1.0 conflicts with @version: 1.1
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#ter42", // Keywords may not be redefined in 1.0
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#tes01", // Using an array value for @context is illegal in JSON-LD 1.0
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#tso01", // @import is invalid in 1.0.
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#ttn01", // @type: @none is illegal in 1.0.
    // A document the algorithm refuses is accepted. #2395
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#t0123", // Value objects including invalid literal datatype IRIs are rejected
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#tc030", // @propagate must be boolean valued
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#tc032", // Unused embedded context with error.
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#tc033", // Unused context with an embedded context error.
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#tdi08", // @direction must be one of ltr or rtl
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#tdi09", // @direction is incompatible with @type
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#tec01", // Invalid keyword in term definition
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#tec02", // Term definition on @type with empty map
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#ten05", // does not allow a keyword other than @nest for the value of @nest
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#ten06", // does not allow @nest with @reverse
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#tep03", // @version must be 1.1
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#ter01", // Keywords cannot be aliased to other keywords
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#ter07", // Invalid base IRI
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#ter09", // Invalid default language
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#ter10", // Cyclic IRI mapping
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#ter13", // Invalid type mapping (not absolute IRI)
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#ter14", // Invalid reverse property (contains @id)
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#ter15", // Invalid IRI mapping (@reverse not a string)
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#ter17", // Invalid reverse property (invalid @container)
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#ter18", // Invalid IRI mapping (@id not a string)
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#ter19", // Invalid keyword alias (@context)
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#ter20", // Invalid IRI mapping (no vocab mapping)
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#ter22", // Invalid language mapping
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#ter23", // Invalid IRI mapping (relative IRI in @type)
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#ter26", // Colliding keywords
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#ter27", // Invalid @id value
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#ter28", // Invalid type value
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#ter29", // Invalid value object value
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#ter30", // Invalid language-tagged string
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#ter31", // Invalid @index value
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#ter34", // Invalid reverse property value (in @reverse)
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#ter35", // Invalid language map value
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#ter36", // Invalid reverse property value (through coercion)
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#ter37", // Invalid value object (unexpected keyword)
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#ter39", // Invalid language-tagged value
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#ter40", // Invalid typed value
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#ter41", // Invalid set or list object
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#ter43", // Term definition with @id: @type
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#ter44", // Redefine terms looking like compact IRIs
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#ter48", // Invalid term as relative IRI
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#ter49", // A relative IRI cannot be used as a prefix
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#ter50", // Invalid reverse id
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#ter51", // Invalid value object value using a value alias
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#ter52", // Definition for the empty term
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#ter53", // Invalid prefix value
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#ter54", // Invalid value object, multiple values for @type.
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#ter56", // Invalid redefinition of @context keyword.
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#tes02", // Mapping @container: [@list, @set] is invalid
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#tin07", // Error if @included value is a string
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#tin08", // Error if @included value is a value object
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#tin09", // Error if @included value is a list object
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#tpi02", // error if @container does not include @index for property-valued index
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#tpr33", // Fails if trying to declare a keyword alias as prefix.
    // `@protected` is not enforced. #2395
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#tpr01", // Protect a term
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#tpr03", // Protect all terms in context
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#tpr04", // Do not protect term with @protected: false
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#tpr05", // Clear active context with protected terms from an embedded context
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#tpr06", // Clear active context of protected terms from a term.
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#tpr08", // Term with protected scoped context.
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#tpr09", // Attempt to redefine term in other protected context.
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#tpr11", // Fail to override protected term.
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#tpr12", // Scoped context fail to override protected term.
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#tpr17", // Fail to override protected terms with type.
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#tpr18", // Fail to override protected terms with type+null+ctx.
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#tpr19", // Mix of protected and unprotected terms.
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#tpr20", // Fail with mix of protected and unprotected terms with type+null+ctx.
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#tpr21", // Fail with mix of protected and unprotected terms with type+null.
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#tpr22", // Check legal overriding of type-scoped protected term from nested node.
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#tpr28", // Fails if trying to redefine a protected null term.
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#tpr30", // Keywords may be protected.
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#tpr31", // Protected keyword aliases cannot be overridden.
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#tpr32", // Protected @type cannot be overridden.
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#tpr42", // Fail if protected flag not retained during redefinition
    // IRI expansion: a relative `@type`, or a reference that is only a query, is not
    // resolved against the base; a relative, compact or term `@vocab`; terms that
    // begin with `@` or `:`; some valid term definitions are refused. #2395
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#t0029", // Relative IRIs
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#t0048", // Terms are ignored in @id
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#t0056", // Use terms with @type: @vocab but not with @type: @id
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#t0059", // Reset @vocab by setting it to null
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#t0060", // Overwrite document base with @base and reset it again
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#t0062", // Various relative IRIs with with @base
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#t0068", // _:suffix values are not a compact IRI
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#t0070", // Compact IRI as term defined using equivalent compact IRI
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#t0072", // Redefine term using @vocab, not itself
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#t0092", // Various relative IRIs as properties with with @vocab: ''
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#t0109", // IRI expansion of fragments including ':'
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#t0110", // Various relative IRIs as properties with with relative @vocab
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#t0111", // Various relative IRIs as properties with with relative @vocab itself relative to an existing vocabulary base
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#t0112", // Various relative IRIs as properties with with relative @vocab relative to another relative vocabulary base
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#t0114", // Expansion allows multiple properties expanding to @type
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#t0117", // A term starting with a colon can expand to a different IRI
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#t0118", // Expanding a value staring with a colon does not treat that value as an IRI
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#t0124", // compact IRI as @vocab
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#t0125", // term as @vocab
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#t0132", // @base does not expand property keys
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#tc021", // type-scoped value mix
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#tc022", // type-scoped property-scoped contexts including @type:@vocab
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#tc023", // composed type-scoped property-scoped contexts including @type:@vocab
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#tc024", // type-scoped + property-scoped + values evaluates against previous context
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#tpr29", // Does not expand a Compact IRI using a non-prefix term.
    // A `@json` term drops a null value, where the algorithm keeps the JSON literal
    // `null`. #2357
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#tjs18", // Expand JSON literal (null)
    // Values the algorithm drops are kept (nulls, unmapped keys, free-floating
    // nodes and values, a value object with no `@value`), and empty arrays are
    // dropped. #2395
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#t0001", // drop free-floating nodes
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#t0003", // drop null and unmapped properties
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#t0008", // @value with @language
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#t0014", // @set of @value objects with keyword aliases
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#t0019", // remove @value = null
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#t0045", // Top-level value objects
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#t0046", // Free-floating nodes
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#t0047", // Free-floating values in sets and free-floating lists
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#t0065", // Drop unmapped keys in reverse map
    // Context and keyword features: a scoped context's term with no `@id` does not
    // take `@vocab`; a type-scoped `null` context or `@base` is ignored; keyword
    // aliases in term definitions; `@none` in language maps; `@type: @none`; `@reverse`
    // and `@nest` edge cases; a nested graph object with no `@id`. #2395
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#t0043", // Using reverse properties inside a @reverse-container
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#t0051", // Expansion of keyword aliases in term definitions
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#tc002", // overriding a term
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#tc003", // property and value with different terms mapping to the same expanded property
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#tc004", // deep @context affects nested nodes
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#tc007", // overriding a term
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#tc012", // deep property-term scoped @context in @type-scoped @context affects nested nodes
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#tc014", // type-scoped context nullification
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#tc015", // type-scoped base
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#tc019", // type-scoped context with multiple property scoped terms
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#tc026", // @propagate: true on type-scoped context
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#tc037", // property-scoped contexts which are alias of @nest
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#tc038", // Bibframe example (poor-mans inferrence)
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#tin06", // json.api example
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#tm009", // language map with @none
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#tm010", // language map with alias of @none
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#ttn02", // @type: @none expands strings as value objects
    // A list of lists is refused (a list member is a value of its property, so it
    // cannot itself be a list), and so is an array nested in a `@set`, which the
    // algorithm flattens. #2395
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#t0004", // optimize @set, keep empty arrays
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#t0015", // collapse set of sets, keep empty lists
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#t0016", // context reset
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#tli03", // @list containing @list (with coercion)
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#tli04", // @list containing empty @list (with coercion)
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#tli05", // coerced @list containing an array
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#tli06", // coerced @list containing an empty array
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#tli07", // coerced @list containing deep arrays
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#tli08", // coerced @list containing deep empty arrays
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#tli09", // coerced @list containing multiple lists
    "https://w3c.github.io/json-ld-api/tests/expand-manifest#tli10", // coerced @list containing mixed list values
];

pub const TO_RDF: &[&str] = &[
    // A remote context is not fetched: there is no document loader, and a string
    // `@context` sets `@vocab` to the string. Documented: compatibility.md (JSON-LD 1.1).
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#tc031", // @context resolutions respects relative URLs.
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#tc034", // Remote scoped context.
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#te126", // A scoped context may include itself recursively (direct)
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#te127", // A scoped context may include itself recursively (indirect)
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#te128", // Two scoped context may include a shared context
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#ter04", // Error dereferencing a remote context
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#ter05", // Invalid remote context
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#tso02", // @import must be a string
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#tso03", // @import overflow
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#tso05", // @propagate: true on type-scoped context with @import
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#tso06", // @propagate: false on property-scoped context with @import
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#tso07", // Protect all terms in sourced context
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#tso10", // Protect terms in sourced context
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#tso12", // @import may not be used in an imported context.
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#tso13", // @import can only reference a single context
    // `@container` `@id`, `@type` and `@graph`, and a property-valued `@index`, are
    // refused. Documented: compatibility.md (JSON-LD 1.1).
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#tc013", // type maps use scoped context from type index and not scoped context from containing
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#tc025", // type-scoped + graph container
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#te079", // expand @graph container
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#te080", // expand [@graph, @set] container
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#te081", // Creates an @graph container if value is a graph
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#te082", // expand [@graph, @index] container
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#te083", // expand [@graph, @index, @set] container
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#te084", // Do not expand [@graph, @index] container if value is a graph
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#te085", // expand [@graph, @id] container
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#te086", // expand [@graph, @id, @set] container
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#te087", // Do not expand [@graph, @id] container if value is a graph
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#te093", // expand @graph container (multiple objects)
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#te094", // expand [@graph, @set] container (multiple objects)
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#te095", // Creates an @graph container if value is a graph (multiple objects)
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#te096", // expand [@graph, @index] container (multiple indexed objects)
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#te097", // expand [@graph, @index, @set] container (multiple objects)
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#te098", // Do not expand [@graph, @index] container if value is a graph (multiple objects)
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#te099", // expand [@graph, @id] container (multiple objects)
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#te100", // expand [@graph, @id, @set] container (multiple objects)
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#te101", // Do not expand [@graph, @id] container if value is a graph (multiple objects)
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#te102", // Expand @graph container if value is a graph (multiple objects)
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#te103", // Expand @graph container if value is a graph (multiple graphs)
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#te104", // Creates an @graph container if value is a graph (mixed graph and object)
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#te105", // Do not expand [@graph, @index] container if value is a graph (mixed graph and object)
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#te106", // Do not expand [@graph, @id] container if value is a graph (mixed graph and object)
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#te107", // expand [@graph, @index] container (indexes with multiple objects)
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#te108", // expand [@graph, @id] container (multiple ids and objects)
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#tm001", // Adds @id to object not having an @id
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#tm002", // Retains @id in object already having an @id
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#tm003", // Adds @type to object not having an @type
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#tm004", // Prepends @type in object already having an @type
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#tm005", // Adds expanded @id to object
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#tm006", // Adds vocabulary expanded @type to object
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#tm007", // Adds document expanded @type to object
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#tm008", // When type is in a type map
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#tm011", // id map with @none
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#tm012", // type map with alias of @none
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#tm013", // graph index map with @none
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#tm014", // graph index map with alias @none
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#tm015", // graph id index map with aliased @none
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#tm016", // graph id index map with aliased @none
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#tm017", // string value of type map expands to node reference
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#tm018", // string value of type map expands to node reference with @type: @id
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#tm019", // string value of type map expands to node reference with @type: @vocab
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#tpi04", // error if @index is not a string for property-valued index
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#tpi06", // property-valued index expands to property value, instead of @index (value)
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#tpi07", // property-valued index appends to property value, instead of @index (value)
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#tpi08", // property-valued index expands to property value, instead of @index (node)
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#tpi09", // property-valued index appends to property value, instead of @index (node)
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#tpi10", // property-valued index does not output property for @none
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#tpi11", // property-valued index adds property to graph object
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#tpr25", // Allows redefinition of terms with scoped contexts using same definitions.
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#tpr43", // Clear protection in @graph @container with null context.
    // A key in keyword form that is no keyword (`"@ignoreMe"`) is kept on the node,
    // where the algorithm drops it, and a transaction refuses it. Documented:
    // compatibility.md (JSON-LD 1.1).
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#te119", // Ignore some terms with @, allow others.
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#te120", // Ignore some values of @id with @, allow others.
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#te121", // Ignore some values of @reverse with @, allow others.
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#te122", // Ignore some IRIs when that start with @ when expanding.
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#tpr34", // Ignores a non-keyword term starting with '@'
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#tpr35", // Ignores a non-keyword term starting with '@' (with @vocab)
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#tpr36", // Ignores a term mapping to a value in the form of a keyword.
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#tpr37", // Ignores a term mapping to a value in the form of a keyword (with @vocab).
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#tpr38", // Ignores a term mapping to a value in the form of a keyword (@reverse).
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#tpr39", // Ignores a term mapping to a value in the form of a keyword (@reverse with @vocab).
    // A node with several types applies their scoped contexts in the order
    // written, and `@propagate` is honored on a type's scoped context only.
    // Documented: compatibility.md (JSON-LD 1.1); #2080.
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#tc011", // orders @type terms when applying scoped contexts
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#tc017", // multiple type-scoped contexts are properly reverted
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#tc018", // multiple type-scoped types resolved against previous context
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#tc027", // @propagate: false on property-scoped context
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#tc028", // @propagate: false on embedded context
    // Properties beside a top-level `@graph`, with no `@id`, are transaction
    // metadata. Documented: docs/transactions/overview.md.
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#t0117", // Dataset from node with embedded named graph (bnode)
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#te021", // do not remove @graph at top-level if not only property
    // The `rdfDirection` option is not supported: a direction is kept in the
    // language tag (`"x"@en--rtl`), and dropped with no language. Documented:
    // compatibility.md (base-direction tags).
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#tdi09", // rdfDirection: i18n-datatype with direction and no language
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#tdi10", // rdfDirection: i18n-datatype with direction and language
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#tdi11", // rdfDirection: compound-literal with direction and no language
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#tdi12", // rdfDirection: compound-literal with direction and language
    // There is no JSON-LD 1.0 processing mode (`processingMode: json-ld-1.0`): a
    // document is processed as JSON-LD 1.1. Documented: compatibility.md (JSON-LD 1.1).
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#tc029", // @propagate is invalid in 1.0
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#te075", // @vocab as blank node identifier
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#tep02", // processingMode json-ld-1.0 conflicts with @version: 1.1
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#ter42", // Keywords may not be redefined in 1.0
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#tso01", // @import is invalid in 1.0.
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#ttn01", // @type: @none is illegal in 1.0.
    // A document the algorithm refuses is accepted. #2395
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#tc030", // @propagate must be boolean valued
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#tc032", // Unused embedded context with error.
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#tc033", // Unused context with an embedded context error.
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#tdi08", // @direction must be one of ltr or rtl
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#te123", // Value objects including invalid literal datatype IRIs are rejected
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#tec01", // Invalid keyword in term definition
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#tec02", // Term definition on @type with empty map
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#ten05", // does not allow a keyword other than @nest for the value of @nest
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#ten06", // does not allow @nest with @reverse
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#tep03", // @version must be 1.1
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#ter01", // Keywords cannot be aliased to other keywords
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#ter07", // Invalid base IRI
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#ter09", // Invalid default language
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#ter10", // Cyclic IRI mapping
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#ter13", // Invalid type mapping (not absolute IRI)
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#ter14", // Invalid reverse property (contains @id)
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#ter15", // Invalid IRI mapping (@reverse not a string)
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#ter17", // Invalid reverse property (invalid @container)
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#ter18", // Invalid IRI mapping (@id not a string)
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#ter19", // Invalid keyword alias (@context)
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#ter20", // Invalid IRI mapping (no vocab mapping)
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#ter22", // Invalid language mapping
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#ter23", // Invalid IRI mapping (relative IRI in @type)
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#ter26", // Colliding keywords
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#ter27", // Invalid @id value
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#ter28", // Invalid type value
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#ter29", // Invalid value object value
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#ter30", // Invalid language-tagged string
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#ter31", // Invalid @index value
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#ter35", // Invalid language map value
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#ter37", // Invalid value object (unexpected keyword)
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#ter39", // Invalid language-tagged value
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#ter40", // Invalid typed value
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#ter43", // Term definition with @id: @type
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#ter44", // Redefine terms looking like compact IRIs
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#ter48", // Invalid term as relative IRI
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#ter49", // A relative IRI cannot be used as a prefix
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#ter50", // Invalid reverse id
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#ter51", // Invalid value object value using a value alias
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#ter52", // Definition for the empty term
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#ter53", // Invalid prefix value
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#ter54", // Invalid value object, multiple values for @type.
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#ter56", // Invalid redefinition of @context keyword.
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#tpi02", // error if @container does not include @index for property-valued index
    // `@protected` is not enforced. #2395
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#tpr01", // Protect a term
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#tpr03", // Protect all terms in context
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#tpr04", // Do not protect term with @protected: false
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#tpr05", // Clear active context with protected terms from an embedded context
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#tpr06", // Clear active context of protected terms from a term.
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#tpr08", // Term with protected scoped context.
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#tpr09", // Attempt to redefine term in other protected context.
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#tpr11", // Fail to override protected term.
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#tpr12", // Scoped context fail to override protected term.
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#tpr17", // Fail to override protected terms with type.
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#tpr18", // Fail to override protected terms with type+null+ctx.
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#tpr19", // Mix of protected and unprotected terms.
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#tpr20", // Fail with mix of protected and unprotected terms with type+null+ctx.
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#tpr21", // Fail with mix of protected and unprotected terms with type+null.
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#tpr22", // Check legal overriding of type-scoped protected term from nested node.
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#tpr28", // Fails if trying to redefine a protected null term.
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#tpr30", // Keywords may be protected.
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#tpr31", // Protected keyword aliases cannot be overridden.
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#tpr32", // Protected @type cannot be overridden.
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#tpr42", // Fail if protected flag not retained during redefinition
    // IRI expansion: a relative `@type`, or a reference that is only a query, is not
    // resolved against the base; a relative, compact or term `@vocab`; terms that
    // begin with `@` or `:`; some valid term definitions are refused. #2395
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#t0120", // IRI Resolution (0)
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#t0121", // IRI Resolution (1)
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#t0122", // IRI Resolution (2)
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#t0123", // IRI Resolution (3)
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#t0124", // IRI Resolution (4)
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#t0125", // IRI Resolution (5)
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#t0126", // IRI Resolution (6)
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#tc021", // type-scoped value mix
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#tc022", // type-scoped property-scoped contexts including @type:@vocab
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#tc023", // composed type-scoped property-scoped contexts including @type:@vocab
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#tc024", // type-scoped + property-scoped + values evaluates against previous context
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#te029", // Relative IRIs
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#te048", // Terms are ignored in @id
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#te056", // Use terms with @type: @vocab but not with @type: @id
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#te059", // Reset @vocab by setting it to null
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#te060", // Overwrite document base with @base and reset it again
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#te062", // Various relative IRIs with with @base
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#te068", // _::sufffix not a compact IRI
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#te070", // Redefine compact IRI with itself
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#te072", // Redefine term using @vocab, not itself
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#te092", // Various relative IRIs as properties with with @vocab: ''
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#te109", // IRI expansion of fragments including ':'
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#te110", // Various relative IRIs as properties with with relative @vocab
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#te111", // Various relative IRIs as properties with with relative @vocab itself relative to an existing vocabulary base
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#te112", // Various relative IRIs as properties with with relative @vocab relative to another relative vocabulary base
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#te114", // Expansion allows multiple properties expanding to @type
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#te117", // A term starting with a colon can expand to a different IRI
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#te118", // Expanding a value staring with a colon does not treat that value as an IRI
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#te124", // compact IRI as @vocab
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#te125", // term as @vocab
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#tpr29", // Does not expand a Compact IRI using a non-prefix term.
    // A `@json` term drops a null value, where the algorithm keeps the JSON literal
    // `null`. #2357
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#tjs18", // Transform JSON literal (null)
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#tjs22", // Transform JSON literal (null) aleady in expanded form.
    // Values the algorithm drops are kept (nulls, unmapped keys, free-floating
    // nodes and values, a value object with no `@value`), and empty arrays are
    // dropped. #2395
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#te003", // drop null and unmapped properties
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#te008", // @value with @language
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#te045", // Top-level value objects are removed
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#te046", // Free-floating nodes are removed
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#te047", // Remove free-floating set values and lists
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#te065", // Keys that are not mapped to an IRI in a reverse-map are dropped
    // Context and keyword features: a scoped context's term with no `@id` does not
    // take `@vocab`; a type-scoped `null` context or `@base` is ignored; keyword
    // aliases in term definitions; `@none` in language maps; `@type: @none`; `@reverse`
    // and `@nest` edge cases; a nested graph object with no `@id`. #2395
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#tc002", // overriding a term
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#tc003", // property and value with different terms mapping to the same expanded property
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#tc004", // deep @context affects nested nodes
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#tc007", // overriding a term
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#tc012", // deep property-term scoped @context in @type-scoped @context affects nested nodes
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#tc014", // type-scoped context nullification
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#tc015", // type-scoped base
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#tc019", // type-scoped context with multiple property scoped terms
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#tc026", // @propagate: true on type-scoped context
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#tc037", // property-scoped contexts which are alias of @nest
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#tc038", // Bibframe example (poor-mans inferrence)
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#te020", // do not remove @graph if not at top-level
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#te043", // Using reverse properties inside a @reverse-container
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#te051", // Expansion of keyword aliases in term definitions
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#tin06", // json.api example
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#tm009", // language map with @none
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#tm010", // language map with alias of @none
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#ttn02", // @type: @none expands strings as value objects
    // A list of lists is refused (a list member is a value of its property, so it
    // cannot itself be a list), and so is an array nested in a `@set`, which the
    // algorithm flattens. #2395
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#te004", // optimize @set, keep empty arrays
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#te015", // collapse set of sets, keep empty lists
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#te016", // context reset
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#tli01", // @list containing @list
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#tli02", // @list containing empty @list
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#tli03", // @list containing @list (with coercion)
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#tli04", // @list containing empty @list (with coercion)
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#tli05", // coerced @list containing an array
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#tli06", // coerced @list containing an empty array
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#tli07", // coerced @list containing deep arrays
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#tli08", // coerced @list containing deep empty arrays
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#tli09", // coerced @list containing multiple lists
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#tli10", // coerced @list containing mixed list values
    // toRdf: a statement with an invalid IRI or language tag, and a list member that
    // is a relative IRI, are not dropped. #2395
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#tli12", // List with bad @base.
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#tli14", // List with null @base.
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#twf01", // Triples including invalid subject IRIs are rejected
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#twf02", // Triples including invalid predicate IRIs are rejected
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#twf03", // Triples including invalid object IRIs are rejected
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#twf04", // Triples including invalid type IRIs are rejected
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#twf05", // Triples including invalid language tags are rejected
    "https://w3c.github.io/json-ld-api/tests/toRdf-manifest#twf07", // Triples including invalid graph name IRIs are rejected
];
