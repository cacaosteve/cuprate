/// Txpool error.
#[derive(thiserror::Error, Debug)]
pub enum TxPoolError {
    #[error("{0}")]
    Fjall(#[from] fjall::Error),
    #[error("Required key not found")]
    NotFound,
    #[error("Only an I2P-first local transaction may record clearnet fallback")]
    InvalidRelayTransition,
}
