pub mod client;
pub mod config;
pub mod events;
mod local;
pub mod service;
pub mod web;

//TODO: when a split is created or deleted, we need to broadcast the event to search
// so that they can download or delete from their local fs if they are responsible
// The storer node:
//  - creates splits
//  - merges splits
//  - run retention policy to delete splits
