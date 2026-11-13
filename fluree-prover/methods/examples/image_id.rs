//! Prints the guest's image id as `fluree-prover --image-id` does, without
//! building the prover.

fn main() {
    let words: Vec<String> = fluree_prover_methods::REDACT_ID
        .iter()
        .map(|w| format!("{w:#010x}"))
        .collect();
    println!("{}", words.join(" "));
}
