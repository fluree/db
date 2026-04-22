# fluree completions

Generate shell completions.

## Usage

```bash
fluree completions <SHELL>
```

## Arguments

| Argument | Description |
|----------|-------------|
| `<SHELL>` | Shell to generate completions for |

## Supported Shells

- `bash`
- `zsh`
- `fish`
- `powershell`
- `elvish`

## Description

Generates shell completion scripts that enable tab-completion for `fluree` commands, options, and arguments.

## Installation

### Bash

```bash
# Add to ~/.bashrc
eval "$(fluree completions bash)"

# Or save to a file
fluree completions bash > /etc/bash_completion.d/fluree
```

### Zsh

```bash
# Add to ~/.zshrc
eval "$(fluree completions zsh)"

# Or save to completions directory
fluree completions zsh > ~/.zfunc/_fluree
# Then add to ~/.zshrc: fpath=(~/.zfunc $fpath)
```

### Fish

```bash
fluree completions fish > ~/.config/fish/completions/fluree.fish
```

### PowerShell

```powershell
# Add to your PowerShell profile
fluree completions powershell | Out-String | Invoke-Expression
```

## Examples

```bash
# Generate bash completions
fluree completions bash

# Generate zsh completions and save
fluree completions zsh > ~/.zfunc/_fluree
```

## Usage After Installation

After installing completions, you can use tab to complete:

```bash
fluree <TAB>        # Shows all commands
fluree que<TAB>     # Completes to "query"
fluree query --<TAB> # Shows available options
```
