use clap::CommandFactory;
use clap_complete::generate;

pub fn run(shell: clap_complete::Shell) {
    let mut cmd = crate::cli::Cli::command();
    generate(shell, &mut cmd, "fluree", &mut std::io::stdout());
}
