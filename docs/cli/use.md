# fluree use

Set the active ledger.

## Usage

```bash
fluree use <LEDGER>
```

## Arguments

| Argument | Description |
|----------|-------------|
| `<LEDGER>` | Ledger name to set as active |

## Description

Sets the specified ledger as the active ledger. Subsequent commands that don't specify a ledger will use this one.

## Examples

```bash
# Switch to a different ledger
fluree use production

# Verify with info
fluree info
```

## Output

```
Active ledger set to 'production'
```

## Errors

If the ledger doesn't exist:
```
error: ledger 'nonexistent' not found
```

## See Also

- [list](list.md) - List all ledgers
- [create](create.md) - Create a new ledger
