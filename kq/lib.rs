// Re-export modules from separate crates for backward compatibility
pub mod schema {
    pub use kq_schema::*;
}

pub mod synthetic {
    pub use kq_synthetic::*;
}

pub mod memory {
    pub use kq_memory::*;
}

pub mod loader {
    pub use kq_loader::*;
}

pub mod query {
    pub use kq_query::*;
}

pub mod output {
    pub use kq_output::*;
}

pub mod cli {
    pub use kq_cli::*;
    pub use kq_cli_interactive::*;
}

pub mod engine_setup {
    pub use kq_engine_setup::*;
}
