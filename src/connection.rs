//! Provides the [Connection] trait for using database connections, and the [AnyConnection]
//! struct, providing a handy generic wrapper around [Connection] implementations.

use async_trait::async_trait;
