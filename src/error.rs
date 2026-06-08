use crate::data::DataError;
use std::net::AddrParseError;
use std::num::TryFromIntError;

#[derive(Debug)]
pub enum TokadError {
    // Boundary / Serialization Errors
    Serialization(DataError),

    // Networking / gRPC Errors
    ConnectionFailed(String),
    NetworkStatus(tonic::Status),
    AddressParse(AddrParseError),
    PortOverflow(TryFromIntError),

    // Routing / DHT Protocol Errors
    NodeOffline(u128),
    ValueNotFound(u128),
    EmptyRoutingTable,

    // Local Systems / Storage Errors
    StorageLockPoisoned,
    Io(std::io::Error),
}

impl std::fmt::Display for TokadError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TokadError::Serialization(e) => write!(f, "Serialization error: {}", e),
            TokadError::ConnectionFailed(msg) => write!(f, "Connection failed: {}", msg),
            TokadError::NetworkStatus(status) => write!(f, "Network gRPC error: {}", status),
            TokadError::AddressParse(e) => write!(f, "Address parse error: {}", e),
            TokadError::PortOverflow(e) => write!(f, "Port overflow: {}", e),
            TokadError::NodeOffline(id) => write!(f, "Node {} is offline", id),
            TokadError::ValueNotFound(key) => write!(f, "Value not found for key: {}", key),
            TokadError::EmptyRoutingTable => write!(f, "Routing table is empty"),
            TokadError::StorageLockPoisoned => write!(f, "Storage lock poisoned"),
            TokadError::Io(e) => write!(f, "I/O error: {}", e),
        }
    }
}

impl std::error::Error for TokadError {}

impl From<DataError> for TokadError {
    fn from(err: DataError) -> Self {
        TokadError::Serialization(err)
    }
}

impl From<tonic::Status> for TokadError {
    fn from(err: tonic::Status) -> Self {
        TokadError::NetworkStatus(err)
    }
}

impl From<AddrParseError> for TokadError {
    fn from(err: AddrParseError) -> Self {
        TokadError::AddressParse(err)
    }
}

impl From<TryFromIntError> for TokadError {
    fn from(err: TryFromIntError) -> Self {
        TokadError::PortOverflow(err)
    }
}

impl From<std::io::Error> for TokadError {
    fn from(err: std::io::Error) -> Self {
        TokadError::Io(err)
    }
}

impl<T> From<std::sync::PoisonError<T>> for TokadError {
    fn from(_err: std::sync::PoisonError<T>) -> Self {
        TokadError::StorageLockPoisoned
    }
}


impl From<DataError> for tonic::Status {
    fn from(err: DataError) -> Self {
        tonic::Status::invalid_argument(format!("Payload error: {}", err))
    }
}

impl From<TokadError> for tonic::Status {
    fn from(err: TokadError) -> Self {
        match err {
            TokadError::Serialization(e) => {
                tonic::Status::invalid_argument(format!("Payload error: {}", e))
            }
            TokadError::AddressParse(e) => {
                tonic::Status::invalid_argument(format!("Invalid network address: {}", e))
            }
            TokadError::PortOverflow(e) => {
                tonic::Status::invalid_argument(format!("Port number invalid: {}", e))
            }
            TokadError::NodeOffline(id) => {
                tonic::Status::unavailable(format!("Node {} is offline", id))
            }
            TokadError::ValueNotFound(key) => {
                tonic::Status::not_found(format!("Value not found for key: {}", key))
            }
            TokadError::NetworkStatus(status) => status,
            TokadError::ConnectionFailed(msg) => tonic::Status::internal(msg),
            _ => tonic::Status::internal(format!("Internal server error: {:?}", err)),
        }
    }
}
